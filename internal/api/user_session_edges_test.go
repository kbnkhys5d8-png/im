package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/service"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestMixedBatchUserTasksAllComplete(t *testing.T) {
	fixture := newSessionRaceFixtureConfig(t, 1, false)
	const count = 12
	fixture.users.taskEvents = make(chan *eventbus.Event, count)
	done := make(chan error, count)
	var calls atomic.Int32
	for i := 0; i < count; i++ {
		go func(i int) {
			done <- eventbus.User.RunOnUserLeader(context.Background(), "session-race-user", func() error {
				calls.Add(1)
				if i == 3 {
					panic("同批次任务异常")
				}
				return nil
			})
		}(i)
	}
	for i := 0; i < count; i++ {
		<-fixture.users.taskEvents
	}
	conn := sessionRaceConn(100)
	conn.Auth = false
	fixture.real.SetContext(conn)
	fixture.users.AddEvent(conn.Uid, &eventbus.Event{Type: eventbus.EventConnect, Conn: conn,
		Frame: &wkproto.ConnectPacket{UID: conn.Uid, Token: "old-token", DeviceFlag: wkproto.APP, DeviceID: conn.DeviceId, Version: 3},
	})
	other := sessionRaceConn(100)
	other.ConnId = 101
	other.Auth = false
	fixture.users.AddEvent(conn.Uid, &eventbus.Event{Type: eventbus.EventConnRemove, Conn: other})
	if err := fixture.pool.Start(); err != nil {
		t.Fatal(err)
	}
	var failures int
	for i := 0; i < count; i++ {
		select {
		case err := <-done:
			if err != nil {
				failures++
			}
		case <-time.After(3 * time.Second):
			t.Fatal("按 EventType 分组后丢失了同批次任务")
		}
	}
	if calls.Load() != count || failures != 1 {
		t.Fatalf("任务执行不完整: calls=%d failures=%d", calls.Load(), failures)
	}
}

func TestUserPoolStopUnblocksQueuedTask(t *testing.T) {
	fixture := newSessionRaceFixtureConfig(t, 1, false)
	fixture.users.taskEvents = make(chan *eventbus.Event, 1)
	done := make(chan error, 1)
	go func() {
		done <- eventbus.User.RunOnUserLeader(context.Background(), "session-race-user", func() error { t.Error("停机后不应执行任务"); return nil })
	}()
	<-fixture.users.taskEvents
	fixture.pool.Stop()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("停机任务未失败")
		}
	case <-time.After(time.Second):
		t.Fatal("停机后仍等待完成通知")
	}
}

func TestOldTokenCannotRegisterAfterRevocation(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	if response := fixture.update("new-token", 42); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	conn := sessionRaceConn(100)
	conn.Auth = false
	fixture.real.SetContext(conn)
	done := make(chan struct{})
	fixture.observer.afterConnect = func() { close(done) }
	fixture.users.AddEvent(conn.Uid, &eventbus.Event{Type: eventbus.EventConnect, Conn: conn,
		Frame: &wkproto.ConnectPacket{UID: conn.Uid, Token: "old-token", DeviceFlag: wkproto.APP, DeviceID: conn.DeviceId, Version: 3},
	})
	fixture.users.Advance(conn.Uid)
	waitSessionRace(t, done)
	if conn.Auth || fixture.users.ConnById(conn.Uid, conn.NodeId, conn.ConnId) != nil {
		t.Fatal("撤销后的旧 Token 又登记成功")
	}
}

func TestValidForwardedConnackWriteAndClosePreserveSession(t *testing.T) {
	for _, isJSON := range []bool{false, true} {
		t.Run(fmt.Sprint(isJSON), func(t *testing.T) {
			fixture := newSessionRaceFixture(t)
			fixture.cluster.leader.Store(2)
			preAuth := sessionRaceConn(100)
			preAuth.Auth, preAuth.IsJsonRpc, preAuth.ProtoVersion = false, isJSON, 3
			fixture.real.SetContext(preAuth)
			manager := service.ConnManager.(*sessionRaceConnManager)
			manager.ws = isJSON
			data, err := preAuth.Encode()
			if err != nil {
				t.Fatal(err)
			}
			authed := &eventbus.Conn{}
			if err := authed.Decode(data); err != nil {
				t.Fatal(err)
			}
			authed.Auth = true
			done := make(chan struct{})
			fixture.observer.afterConnack = func() { close(done) }
			fixture.handler.OnMessage(sessionRaceForward(t, &eventbus.Event{Type: eventbus.EventConnack, Conn: authed,
				Frame: &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonSuccess}, SourceNodeId: 2,
			}, 2))
			waitSessionRace(t, done)
			current := fixture.real.Context().(*eventbus.Conn)
			if !current.Auth || !current.SameSession(preAuth) || current.IsJsonRpc != isJSON {
				t.Fatal("合法跨节点 Connack 被丢弃或丢失认证前会话身份")
			}
			fixture.handler.OnEvent(&eventbus.UserContext{Uid: current.Uid, EventType: eventbus.EventConnWriteFrame,
				Events: []*eventbus.Event{{Conn: current, Frame: &wkproto.PongPacket{}}},
			})
			if fixture.real.writes.Load() != 1 {
				t.Fatal("合法消息写入被代际检查误丢")
			}
			if isJSON {
				if err := ingress.New().RevokeConn(current); err != nil {
					t.Fatal(err)
				}
				if fixture.real.textWrites.Load() != 2 || fixture.real.flushes.Load() != 1 {
					t.Fatal("JSON-RPC 撤销未保留原断开通知并刷新")
				}
			} else {
				fixture.handler.OnEvent(&eventbus.UserContext{Uid: current.Uid, EventType: eventbus.EventConnClose, Events: []*eventbus.Event{{Conn: current}}})
			}
			if !fixture.real.closed.Load() {
				t.Fatal("合法当前会话关闭被误丢")
			}
		})
	}
}

func TestRevokedOrUnregisteredSessionCannotSend(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	conn := sessionRaceConn(100)
	fixture.users.UpdateConn(conn)
	conn.Revoked.Store(true)
	for _, registered := range []bool{true, false} {
		if !registered {
			fixture.users.RemoveConn(conn)
		}
		fixture.handler.OnEvent(&eventbus.UserContext{Uid: conn.Uid, EventType: eventbus.EventOnSend,
			// PING 的响应可直接观察是否穿过认证入场，不触碰消息业务或存储。
			Events: []*eventbus.Event{{Conn: conn, Frame: &wkproto.PingPacket{}}},
		})
	}
	fixture.users.mu.Lock()
	defer fixture.users.mu.Unlock()
	for _, event := range fixture.users.events {
		if _, ok := event.Frame.(*wkproto.PongPacket); ok {
			t.Fatal("撤销或未登记的会话仍进入业务处理")
		}
	}
}

func TestSaturatedWorkerPoolStillCompletesUserTask(t *testing.T) {
	newSessionRaceFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	first := make(chan error, 1)
	go func() {
		first <- eventbus.User.RunOnUserLeader(context.Background(), "busy-user", func() error { close(entered); <-release; return nil })
	}()
	waitSessionRace(t, entered)
	second := make(chan error, 1)
	go func() {
		second <- eventbus.User.RunOnUserLeader(context.Background(), "other-user", func() error { return nil })
	}()
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker 满时 UID 任务被丢弃或拒绝后仍永久等待")
	}
	once.Do(func() { close(release) })
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestCanceledHTTPTaskDoesNotWriteAfterHandlerReturns(t *testing.T) {
	fixture := newSessionRaceFixtureConfig(t, 1, false)
	fixture.users.taskEvents = make(chan *eventbus.Event, 2)
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/user/token/fenced", strings.NewReader(`{"uid":"session-race-user","device_flag":0,"device_level":1,"token":"canceled-token","token_version":44}`)).WithContext(ctx)
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		fixture.router.GetGinRoute().ServeHTTP(response, request)
		done <- response
	}()
	<-fixture.users.taskEvents
	cancel()
	select {
	case response := <-done:
		if response.Code == http.StatusOK {
			t.Fatal("取消的 HTTP 任务仍成功")
		}
	case <-time.After(time.Second):
		t.Fatal("请求取消未唤醒 handler")
	}
	if err := fixture.pool.Start(); err != nil {
		t.Fatal(err)
	}
	if response := fixture.update("new-token", 42); response.Code != http.StatusOK {
		t.Fatalf("晚到任务覆盖了新请求或复用 HTTP 上下文: %s", response.Body.String())
	}
	device, _ := fixture.db.GetDevice("session-race-user", 0)
	if device.TokenVersion != 42 {
		t.Fatal("取消的晚到任务写入了 Token")
	}
}

func TestRemoteRevokeFailureFailsClosedAndRetryPreservesNewSession(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	old := sessionRaceConn(100)
	old.NodeId = 2
	fixture.users.UpdateConn(old)
	fixture.cluster.requestErr = errors.New("模拟源节点不可达")
	if response := fixture.update("new-token", 42); response.Code != http.StatusBadRequest {
		t.Fatalf("首次撤销失败应返回 400 而非成功或版本冲突: %d %s", response.Code, response.Body.String())
	}
	device, err := fixture.db.GetDevice(old.Uid, uint64(old.DeviceFlag))
	if err != nil || device.TokenVersion != 42 || device.Token != "new-token" {
		t.Fatalf("首次失败后应已保存候选 Token 及版本: err=%v", err)
	}
	if response := fixture.update("new-token", 42); response.Code != http.StatusBadRequest {
		t.Fatalf("同版本同 Token 的撤销重试失败应仍为 400，不得误报 409: %d %s", response.Code, response.Body.String())
	}
	if !old.Revoked.Load() {
		t.Fatal("源节点不可达时 leader 未先禁止旧会话入场")
	}
	current := sessionRaceConn(100)
	current.ConnId = 101
	current.AuthTokenHash = eventbus.TokenFingerprint("new-token")
	fixture.users.UpdateConn(current)
	fixture.cluster.requestErr = nil
	if response := fixture.update("new-token", 42); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	if fixture.users.ConnById(old.Uid, old.NodeId, old.ConnId) != nil || !fixture.cluster.remoteClosed.Load() {
		t.Fatal("幂等重试未补完旧会话关闭")
	}
	if fixture.users.ConnById(current.Uid, current.NodeId, current.ConnId) != current || current.Revoked.Load() {
		t.Fatal("重试误伤相同当前 Token 的新登录")
	}
}

func TestSourceDiscoveryFailureRevokesKnownSessionBeforeReturning(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	old := sessionRaceConn(100)
	fixture.users.UpdateConn(old)
	fixture.real.SetContext(old)
	current := sessionRaceConn(100)
	current.ConnId = 101
	current.AuthTokenHash = eventbus.TokenFingerprint("new-token")
	fixture.users.UpdateConn(current)
	// 让远端真实出现在节点清单中，错误发生在查询源会话而不是随后撤销阶段。
	fixture.cluster.peers = map[uint64][]*eventbus.Conn{2: nil}
	fixture.cluster.requestErr = errors.New("模拟源会话查询失败")
	if response := fixture.update("new-token", 42); response.Code != http.StatusBadRequest {
		t.Fatalf("源会话查询失败仍返回成功: %d %s", response.Code, response.Body.String())
	}
	if !old.Revoked.Load() {
		t.Fatal("源会话查询失败后，已知旧会话仍可进入消息处理")
	}
	if current.Revoked.Load() {
		t.Fatal("源会话查询失败误伤了当前 Token 的连接")
	}
	fixture.handler.OnEvent(&eventbus.UserContext{Uid: old.Uid, EventType: eventbus.EventOnSend,
		Events: []*eventbus.Event{{Type: eventbus.EventOnSend, Conn: old, Frame: &wkproto.PingPacket{}}},
	})
	fixture.users.mu.Lock()
	for _, event := range fixture.users.events {
		if _, ok := event.Frame.(*wkproto.PongPacket); ok && event.Conn.SameSession(old) {
			fixture.users.mu.Unlock()
			t.Fatal("源会话查询失败后旧连接仍被确认可用")
		}
	}
	fixture.users.mu.Unlock()
	fixture.cluster.requestErr = nil
	if response := fixture.update("new-token", 42); response.Code != http.StatusOK {
		t.Fatalf("查询恢复后的同版本重试失败: %d %s", response.Code, response.Body.String())
	}
	if !fixture.real.closed.Load() || fixture.users.ConnById(old.Uid, old.NodeId, old.ConnId) != nil {
		t.Fatal("查询恢复后未补完旧连接关闭")
	}
	if fixture.users.ConnById(current.Uid, current.NodeId, current.ConnId) != current || current.Revoked.Load() {
		t.Fatal("同版本重试影响当前 Token 的连接")
	}
}

func TestDelayedRevokeRPCDoesNotTouchReplacement(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	fixture.cluster.leader.Store(2)
	old, current := sessionRaceConn(100), sessionRaceConn(100)
	fixture.users.UpdateConn(current)
	fixture.real.SetContext(current)
	data, _ := old.Encode()
	forwarded := &eventbus.Conn{}
	if err := forwarded.Decode(data); err != nil {
		t.Fatal(err)
	}
	if err := ingress.New().RevokeConn(forwarded); err != nil {
		t.Fatal(err)
	}
	if fixture.real.closed.Load() || fixture.real.writes.Load() != 0 || fixture.users.ConnById(current.Uid, current.NodeId, current.ConnId) != current {
		t.Fatal("延迟撤销 RPC 影响了同秒复用连接号的新登录")
	}
}

func TestKickNotificationFailureDoesNotDelayClose(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	conn := sessionRaceConn(100)
	fixture.users.UpdateConn(conn)
	fixture.real.SetContext(conn)
	fixture.real.writeErr = errors.New("模拟通知写入失败")
	if response := fixture.update("new-token", 42); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	if !fixture.real.closed.Load() {
		t.Fatal("通知写入失败后没有立即关闭旧会话")
	}
}

func TestNewEntryRejectsConnackStrippedByLegacyLeader(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	fixture.cluster.leader.Store(2)
	preAuth := sessionRaceConn(100)
	preAuth.Auth = false
	fixture.real.SetContext(preAuth)
	data, _ := preAuth.Encode()
	// 模拟旧 leader 读取后按旧格式重编码，往返会丢失新增的内部扩展。
	data = data[:len(data)-2-len(preAuth.SessionId)-2-len(preAuth.AuthTokenHash)]
	legacy := &eventbus.Conn{}
	if err := legacy.Decode(data); err != nil {
		t.Fatal(err)
	}
	legacy.Auth = true
	done := make(chan struct{})
	fixture.observer.afterConnack = func() { close(done) }
	fixture.handler.OnMessage(sessionRaceForward(t, &eventbus.Event{Type: eventbus.EventConnack, Conn: legacy,
		Frame: &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonSuccess}, SourceNodeId: 2,
	}, 2))
	waitSessionRace(t, done)
	if fixture.real.Context() != preAuth || preAuth.Auth || fixture.users.ConnById(preAuth.Uid, preAuth.NodeId, preAuth.ConnId) != nil {
		t.Fatal("为兼容混版而放行了丢失会话代际的 Connack")
	}
}
