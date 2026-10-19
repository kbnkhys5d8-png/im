package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	userevent "github.com/WuKongIM/WuKongIM/internal/user/event"
	userhandler "github.com/WuKongIM/WuKongIM/internal/user/handler"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestTokenChangeIncludesInFlightRegistration(t *testing.T) {
	for _, phase := range []string{"after-token-verification", "before-registration", "forwarded-connect"} {
		t.Run(phase, func(t *testing.T) {
			fixture := newSessionRaceFixture(t)
			paused, resume := make(chan struct{}), make(chan struct{})
			var resumeOnce sync.Once
			release := func() { resumeOnce.Do(func() { close(resume) }) }
			defer release()
			if phase != "before-registration" {
				fixture.db.channelGate = func() { close(paused); <-resume }
			}
			conn := sessionRaceConn(100)
			connected := make(chan struct{})
			fixture.observer.beforeAdd = func() {
				if phase == "before-registration" {
					close(paused)
					<-resume
				}
			}
			fixture.observer.afterConnect = func() { close(connected) }
			fixture.real.SetContext(conn)
			if phase == "forwarded-connect" {
				conn.NodeId = 2
			}
			event := &eventbus.Event{Type: eventbus.EventConnect, Conn: conn, Frame: &wkproto.ConnectPacket{
				UID: conn.Uid, Token: "old-token", DeviceFlag: wkproto.APP, DeviceID: conn.DeviceId, Version: 3,
			}}
			if phase == "forwarded-connect" {
				fixture.handler.OnMessage(sessionRaceForward(t, event, 2))
			} else {
				fixture.users.AddEvent(conn.Uid, event)
				fixture.users.Advance(conn.Uid)
			}
			waitSessionRace(t, paused)
			updated := make(chan *httptest.ResponseRecorder, 1)
			go func() { updated <- fixture.update("revoke-token", 42) }()
			// 已有实现会先完成撤销；修复后必须等正在登记的连接进入撤销集合。
			var response *httptest.ResponseRecorder
			select {
			case response = <-updated:
			case <-time.After(50 * time.Millisecond):
			}
			release()
			waitSessionRace(t, connected)
			if response == nil {
				select {
				case response = <-updated:
				case <-time.After(3 * time.Second):
					t.Fatal("Token 更新未完成")
				}
			}
			if response.Code != http.StatusOK {
				t.Fatalf("Token 更新失败: %d %s", response.Code, response.Body.String())
			}
			if fixture.users.ConnById(conn.Uid, conn.NodeId, conn.ConnId) != nil {
				t.Fatal("撤销成功后在途旧连接仍登记在线")
			}
			if phase != "forwarded-connect" && !fixture.real.closed.Load() {
				t.Fatal("在途旧连接未实际关闭")
			}
			if phase == "forwarded-connect" && !fixture.cluster.remoteClosed.Load() {
				t.Fatal("远端源节点未确认关闭")
			}
		})
	}
}

func TestDelayedDisconnectKeepsReplacementConnection(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	old, current := sessionRaceConn(100), sessionRaceConn(100)
	fixture.users.UpdateConn(current)
	fixture.real.SetContext(current)
	fixture.handler.OnEvent(&eventbus.UserContext{Uid: old.Uid, EventType: eventbus.EventConnWriteFrame,
		Events: []*eventbus.Event{{Conn: old, Frame: &wkproto.DisconnectPacket{ReasonCode: wkproto.ReasonConnectKick}}},
	})
	if fixture.real.writes.Load() != 0 {
		t.Fatal("旧 disconnect 写入了新连接")
	}
}

func TestForwardedOldCloseKeepsReplacementConnection(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	old, current := sessionRaceConn(100), sessionRaceConn(100)
	fixture.users.UpdateConn(current)
	fixture.real.SetContext(current)
	fixture.handler.OnMessage(sessionRaceForward(t, &eventbus.Event{Type: eventbus.EventConnClose, Conn: old}, 2))
	// main 已在转发层同步丢弃旧代际，不应再等待关闭 handler 被调用。
	fixture.users.mu.Lock()
	queued := len(fixture.users.events)
	fixture.users.mu.Unlock()
	if queued != 0 {
		t.Fatal("旧代际关闭不应进入用户事件队列")
	}
	if fixture.real.closed.Load() {
		t.Fatal("转发层按连接号替换了旧事件，误关新连接")
	}
	if fixture.users.ConnById(current.Uid, current.NodeId, current.ConnId) != current {
		t.Fatal("转发旧关闭移除了新逻辑连接")
	}
}

func sessionRaceForward(t *testing.T, event *eventbus.Event, from uint64) *proto.Message {
	t.Helper()
	data, err := (eventbus.EventBatch{event}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	enc := wkproto.NewEncoder()
	defer enc.End()
	enc.WriteString(event.Conn.Uid)
	enc.WriteUint64(from)
	enc.WriteBytes(data)
	return &proto.Message{MsgType: 2001, Content: enc.Bytes()}
}

func TestDelayedCloseKeepsReplacementConnection(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	old, current := sessionRaceConn(100), sessionRaceConn(100)
	fixture.users.UpdateConn(current)
	fixture.real.SetContext(current)
	fixture.handler.OnEvent(&eventbus.UserContext{
		Uid: old.Uid, EventType: eventbus.EventConnClose, Events: []*eventbus.Event{{Conn: old}},
	})
	if fixture.real.closed.Load() {
		t.Fatal("旧连接延迟关闭误关了复用连接号的新连接")
	}
	if got := fixture.users.ConnById(current.Uid, current.NodeId, current.ConnId); got != current {
		t.Fatal("旧关闭事件移除了新逻辑连接")
	}
}

func TestStaleConnackKeepsReplacementConnection(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	old, current := sessionRaceConn(100), sessionRaceConn(100)
	fixture.users.UpdateConn(current)
	fixture.real.SetContext(current)
	fixture.handler.OnEvent(&eventbus.UserContext{
		Uid: old.Uid, EventType: eventbus.EventConnack,
		Events: []*eventbus.Event{{Conn: old, Frame: &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonSuccess}}},
	})
	if fixture.real.Context() != current {
		t.Fatal("迟到的旧认证回执覆盖了新连接上下文")
	}
	if got := fixture.users.ConnById(current.Uid, current.NodeId, current.ConnId); got != current {
		t.Fatal("迟到回执覆盖了新逻辑连接")
	}
}

func TestLateTokenVersionCannotDisconnectNewLogin(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	if response := fixture.update("new-token", 43); response.Code != http.StatusOK {
		t.Fatalf("新登录失败: %s", response.Body.String())
	}
	current := sessionRaceConn(200)
	fixture.users.UpdateConn(current)
	if response := fixture.update("late-revoke", 42); response.Code != http.StatusConflict {
		t.Fatalf("过时撤销未被拒绝: %d %s", response.Code, response.Body.String())
	}
	device, _ := fixture.db.GetDevice(current.Uid, uint64(current.DeviceFlag))
	if device.Token != "new-token" || device.TokenVersion != 43 {
		t.Fatalf("旧撤销覆盖新版本: version=%d", device.TokenVersion)
	}
	if fixture.users.disconnected(current) {
		t.Fatal("过时撤销踢掉了新登录")
	}
}

func TestCurrentCloseStillClosesConnection(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	current := sessionRaceConn(200)
	fixture.users.UpdateConn(current)
	fixture.real.SetContext(current)
	fixture.handler.OnEvent(&eventbus.UserContext{Uid: current.Uid, EventType: eventbus.EventConnClose, Events: []*eventbus.Event{{Conn: current}}})
	if !fixture.real.closed.Load() {
		t.Fatal("当前连接应正常关闭")
	}
}

func TestTokenChangeClosesOldConnectionBeforeSuccess(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	current := sessionRaceConn(100)
	fixture.users.UpdateConn(current)
	fixture.real.SetContext(current)
	response := fixture.update("revoke-token", 42)
	if response.Code != http.StatusOK {
		t.Fatalf("撤销失败: %s", response.Body.String())
	}
	if !fixture.real.closed.Load() {
		t.Fatal("撤销成功返回后旧连接仍存活，可忽略 Disconnect 继续收发")
	}
	if fixture.real.writes.Load() != 1 {
		t.Fatal("立即关闭丢失了原 Disconnect 踢号通知")
	}
	data := fixture.real.lastData.Load()
	if data == nil {
		t.Fatal("未写入断开帧")
	}
	frame, _, err := eventbus.Proto.DecodeFrame(*data, wkproto.LatestVersion)
	if err != nil {
		t.Fatal(err)
	}
	packet, ok := frame.(*wkproto.DisconnectPacket)
	if !ok || packet.ReasonCode != wkproto.ReasonConnectKick || fixture.real.flushes.Load() != 1 {
		t.Fatal("原踢号原因丢失或未尝试刷新通知")
	}
	fixture.handler.OnEvent(&eventbus.UserContext{Uid: current.Uid, EventType: eventbus.EventConnWriteFrame,
		Events: []*eventbus.Event{{Conn: current, Frame: &wkproto.PongPacket{}}},
	})
	if fixture.real.writes.Load() != 1 {
		t.Fatal("撤销成功后仍向旧会话写入数据")
	}
}

func TestSameTokenRefreshKeepsConnection(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	current := sessionRaceConn(100)
	fixture.users.UpdateConn(current)
	fixture.real.SetContext(current)
	response := fixture.update("old-token", 42)
	if response.Code != http.StatusOK {
		t.Fatalf("同 Token 刷新失败: %s", response.Body.String())
	}
	if fixture.real.closed.Load() || fixture.users.disconnected(current) {
		t.Fatal("同 Token 刷新踢掉正常连接")
	}
}

func TestUserLeaderTaskPanicReturnsError(t *testing.T) {
	newSessionRaceFixture(t)
	done := make(chan error, 1)
	go func() {
		done <- eventbus.User.RunOnUserLeader(context.Background(), "session-race-user", func() error { panic("测试任务异常") })
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("任务 panic 未返回错误")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("任务 panic 导致调用方永久等待")
	}
}

func TestTokenWriteRejectsLeaderChangeAndRetryClosesUnmarkedOldSession(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	fixture.cluster.afterApply = func() { fixture.cluster.leader.Store(2) }
	if response := fixture.update("new-token", 42); response.Code == http.StatusOK {
		t.Fatal("Token 写入期间 leader 切走，仍返回成功")
	}
	fixture.cluster.afterApply = nil
	fixture.cluster.leader.Store(1)
	old := sessionRaceConn(100)
	fixture.users.UpdateConn(old)
	fixture.real.SetContext(old)
	if response := fixture.update("new-token", 42); response.Code != http.StatusOK {
		t.Fatalf("迁移后幂等重试失败: %s", response.Body.String())
	}
	if !fixture.real.closed.Load() || fixture.users.ConnById(old.Uid, old.NodeId, old.ConnId) != nil {
		t.Fatal("幂等重试保留了未标记 Revoked 的旧 Token 会话")
	}
}

func sessionRaceConn(uptime uint64) *eventbus.Conn {
	return &eventbus.Conn{Uid: "session-race-user", NodeId: 1, ConnId: 99, DeviceId: "same-device", DeviceFlag: wkproto.APP, DeviceLevel: wkproto.DeviceLevelMaster, Uptime: uptime, SessionId: wkutil.GenUUID(), AuthTokenHash: eventbus.TokenFingerprint("old-token"), Auth: true}
}

func waitSessionRace(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("等待并发测试检查点超时")
	}
}

type sessionRaceFixture struct {
	db       *sessionRaceDB
	users    *sessionRaceUsers
	real     *sessionRaceRealConn
	handler  *userhandler.Handler
	observer *sessionRaceObserver
	cluster  *sessionRaceCluster
	router   *wkhttp.WKHttp
	pool     *userevent.EventPool
}

func newSessionRaceFixture(t *testing.T) *sessionRaceFixture {
	return newSessionRaceFixtureNode(t, 1)
}

func newSessionRaceFixtureNode(t *testing.T, node uint64) *sessionRaceFixture {
	return newSessionRaceFixtureConfig(t, node, true)
}

func newSessionRaceFixtureConfig(t *testing.T, node uint64, start bool) *sessionRaceFixture {
	t.Helper()
	oldOptions, oldStore, oldCluster, oldPermission := options.G, service.Store, service.Cluster, service.Permission
	oldWebhook, oldUser, oldManager := service.Webhook, eventbus.User, service.ConnManager
	oldTrace := trace.GlobalTrace
	trace.GlobalTrace = trace.New(context.Background(), trace.NewOptions())
	options.G = options.New()
	options.G.Cluster.NodeId, options.G.TokenAuthOn, options.G.DisableEncryption = node, true, true
	options.G.Poller.UserCount, options.G.Poller.UserGoroutine = 1, 1
	db := &sessionRaceDB{device: wkdb.Device{Id: 1, Uid: "session-race-user", DeviceFlag: 0, DeviceLevel: 1, Token: "old-token", TokenVersion: 41}}
	cluster := &sessionRaceCluster{db: db}
	cluster.node = node
	cluster.leader.Store(node)
	service.Store = store.New(store.NewOptions(store.WithDB(db), store.WithSlot(cluster)))
	service.Cluster, service.Permission, service.Webhook = cluster, service.NewPermissionService(nil), &sessionRaceWebhook{}
	handler := userhandler.NewHandler()
	observer := &sessionRaceObserver{Handler: handler}
	pool := userevent.NewEventPool(observer)
	users := &sessionRaceUsers{IUser: pool}
	eventbus.RegisterUser(users)
	real := &sessionRaceRealConn{}
	service.ConnManager = &sessionRaceConnManager{real: real}
	// 真实连接总是先有 UID handler；直接测试回执和关闭时也保留此前提。
	pool.AddEvent("session-race-user", &eventbus.Event{Type: eventbus.EventUnknown})
	if start {
		if err := pool.Start(); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		pool.Stop()
		options.G, service.Store, service.Cluster, service.Permission = oldOptions, oldStore, oldCluster, oldPermission
		service.Webhook, eventbus.User, service.ConnManager = oldWebhook, oldUser, oldManager
		trace.GlobalTrace = oldTrace
	})
	router := wkhttp.New()
	newUser(&Server{timingWheel: timingwheel.NewTimingWheel(time.Millisecond, 64)}).route(router)
	return &sessionRaceFixture{db: db, users: users, real: real, handler: handler, observer: observer, cluster: cluster, router: router, pool: pool}
}

type sessionRaceObserver struct {
	*userhandler.Handler
	beforeAdd    func()
	afterConnect func()
	afterClose   func()
	afterConnack func()
	afterOnSend  func()
}

func (h *sessionRaceObserver) OnEvent(ctx *eventbus.UserContext) {
	add := ctx.AddConn
	ctx.AddConn = func(conn *eventbus.Conn) {
		if h.beforeAdd != nil {
			h.beforeAdd()
		}
		add(conn)
	}
	h.Handler.OnEvent(ctx)
	if ctx.EventType == eventbus.EventConnect && h.afterConnect != nil {
		h.afterConnect()
	}
	if ctx.EventType == eventbus.EventConnClose && h.afterClose != nil {
		h.afterClose()
	}
	if ctx.EventType == eventbus.EventConnack && h.afterConnack != nil {
		h.afterConnack()
	}
	if ctx.EventType == eventbus.EventOnSend && h.afterOnSend != nil {
		h.afterOnSend()
	}
}

func (f *sessionRaceFixture) update(token string, version uint64) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	body := `{"uid":"session-race-user","device_flag":0,"device_level":1,"token":"` + token + `","token_version":` + strconv.FormatUint(version, 10) + `}`
	f.router.GetGinRoute().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/user/token/fenced", strings.NewReader(body)))
	return w
}

type sessionRaceDB struct {
	wkdb.DB
	mu           sync.Mutex
	device       wkdb.Device
	channelReads atomic.Int32
	channelGate  func()
	ban          atomic.Bool
}

func (d *sessionRaceDB) GetDevice(string, uint64) (wkdb.Device, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.device, nil
}
func (d *sessionRaceDB) GetChannel(string, uint8) (wkdb.ChannelInfo, error) {
	if d.channelReads.Add(1) == 1 && d.channelGate != nil {
		d.channelGate()
	}
	return wkdb.ChannelInfo{Ban: d.ban.Load()}, nil
}
func (d *sessionRaceDB) GetUser(uid string) (wkdb.User, error) { return wkdb.User{Uid: uid}, nil }

type sessionRaceCluster struct {
	icluster.ICluster
	icluster.Slot
	db           *sessionRaceDB
	leader       atomic.Uint64
	forwardURL   string
	revokeURL    string
	remoteClosed atomic.Bool
	afterApply   func()
	requestErr   error
	node         uint64
	peers        map[uint64][]*eventbus.Conn
	revoked      []*eventbus.Conn
}

func (*sessionRaceCluster) GetSlotId(string) uint32      { return 1 }
func (c *sessionRaceCluster) SlotLeaderId(uint32) uint64 { return c.leader.Load() }
func (c *sessionRaceCluster) Nodes() []*nodetypes.Node {
	nodes := []*nodetypes.Node{{Id: c.node}}
	for id := range c.peers {
		nodes = append(nodes, &nodetypes.Node{Id: id})
	}
	if c.revokeURL != "" {
		nodes = append(nodes, &nodetypes.Node{Id: 1})
	}
	return nodes
}
func (c *sessionRaceCluster) SlotLeaderOfChannel(string, uint8) (*nodetypes.Node, error) {
	return &nodetypes.Node{Id: c.leader.Load(), ApiServerAddr: c.forwardURL}, nil
}
func (c *sessionRaceCluster) ProposeUntilApplied(_ uint32, data []byte) (*rafttypes.ProposeResp, error) {
	cmd := &store.CMD{}
	if err := cmd.Unmarshal(data); err != nil {
		return nil, err
	}
	device, err := cmd.DecodeCMDDevice()
	if err != nil {
		return nil, err
	}
	c.db.mu.Lock()
	if device.TokenVersion >= c.db.device.TokenVersion {
		c.db.device = device
	}
	c.db.mu.Unlock()
	if c.afterApply != nil {
		c.afterApply()
	}
	return &rafttypes.ProposeResp{}, nil
}
func (c *sessionRaceCluster) Send(_ uint64, message *proto.Message) error {
	if c.forwardURL == "" {
		return nil
	}
	resp, err := http.Post(c.forwardURL+"/test/events", "application/octet-stream", strings.NewReader(string(message.Content)))
	if err == nil {
		resp.Body.Close()
	}
	return err
}

func (c *sessionRaceCluster) RequestWithContext(ctx context.Context, node uint64, path string, data []byte) (*proto.Response, error) {
	if c.requestErr != nil {
		return nil, c.requestErr
	}
	if path == "/wk/ingress/userConns" && c.revokeURL == "" {
		var batch eventbus.EventBatch
		for _, conn := range c.peers[node] {
			batch = append(batch, &eventbus.Event{Conn: conn})
		}
		body, err := batch.Encode()
		return &proto.Response{Status: proto.StatusOK, Body: body}, err
	}
	if path != "/wk/ingress/revokeConn" && path != "/wk/ingress/userConns" {
		return nil, fmt.Errorf("未 mock 的路径: %s", path)
	}
	if c.revokeURL != "" {
		endpoint := "/test/revoke"
		if path == "/wk/ingress/userConns" {
			endpoint = "/test/conns"
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.revokeURL+endpoint, strings.NewReader(string(data)))
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusOK {
			return &proto.Response{Status: proto.StatusError}, nil
		}
		if path == "/wk/ingress/userConns" {
			return &proto.Response{Status: proto.StatusOK, Body: body}, nil
		}
	}
	conn := &eventbus.Conn{}
	if err := conn.Decode(data); err != nil {
		return nil, err
	}
	c.revoked = append(c.revoked, conn)
	c.remoteClosed.Store(true)
	return &proto.Response{Status: proto.StatusOK}, nil
}

type sessionRaceUsers struct {
	eventbus.IUser
	mu         sync.Mutex
	events     []*eventbus.Event
	taskQueued chan struct{}
	taskOnce   sync.Once
	taskEvents chan *eventbus.Event
}

func (u *sessionRaceUsers) AddEvent(uid string, event *eventbus.Event) {
	u.mu.Lock()
	u.events = append(u.events, event)
	if event.Type == eventbus.EventUserLeaderTask && u.taskQueued != nil {
		u.taskOnce.Do(func() { close(u.taskQueued) })
	}
	if event.Type == eventbus.EventUserLeaderTask && u.taskEvents != nil {
		u.taskEvents <- event
	}
	u.mu.Unlock()
	// 保留真实 UID 队列，仅记录写事件以避免测试发送网络数据。
	if event.Type != eventbus.EventConnWriteFrame {
		u.IUser.AddEvent(uid, event)
	}
}
func (u *sessionRaceUsers) disconnected(conn *eventbus.Conn) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, event := range u.events {
		if _, ok := event.Frame.(*wkproto.DisconnectPacket); ok && event.Conn.Equal(conn) {
			return true
		}
	}
	return false
}

type sessionRaceWebhook struct{ service.IWebhook }

func (*sessionRaceWebhook) Online(string, wkproto.DeviceFlag, int64, int, int)  {}
func (*sessionRaceWebhook) Offline(string, wkproto.DeviceFlag, int64, int, int) {}

type sessionRaceRealConn struct {
	wknet.Conn
	context    atomic.Pointer[eventbus.Conn]
	closed     atomic.Bool
	removed    atomic.Bool
	writes     atomic.Int32
	textWrites atomic.Int32
	flushes    atomic.Int32
	lastData   atomic.Pointer[[]byte]
	writeErr   error
}

func (c *sessionRaceRealConn) Context() interface{}         { return c.context.Load() }
func (c *sessionRaceRealConn) SetContext(value interface{}) { c.context.Store(value.(*eventbus.Conn)) }
func (*sessionRaceRealConn) SetMaxIdle(time.Duration)       {}
func (*sessionRaceRealConn) Fd() wknet.NetFd                { return wknet.NetFd{} }
func (c *sessionRaceRealConn) Close() error                 { c.closed.Store(true); return nil }
func (c *sessionRaceRealConn) WriteToOutboundBuffer(data []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.writes.Add(1)
	copy := append([]byte(nil), data...)
	c.lastData.Store(&copy)
	return len(data), nil
}
func (*sessionRaceRealConn) WakeWrite() error { return nil }
func (c *sessionRaceRealConn) Flush() error   { c.flushes.Add(1); return nil }

type sessionRaceConnManager struct {
	service.IConnManager
	real *sessionRaceRealConn
	ws   bool
}

func (m *sessionRaceConnManager) GetConn(id int64) wknet.Conn {
	if id != 99 || m.real.removed.Load() {
		return nil
	}
	if m.ws {
		return &sessionRaceWSConn{m.real}
	}
	return m.real
}
func (m *sessionRaceConnManager) GetConnByFd(int) wknet.Conn { return m.GetConn(99) }
func (m *sessionRaceConnManager) RemoveConn(wknet.Conn)      { m.real.removed.Store(true) }
func (m *sessionRaceConnManager) GetAllConn() []wknet.Conn {
	if m.real.removed.Load() {
		return nil
	}
	return []wknet.Conn{m.real}
}

type sessionRaceWSConn struct{ *sessionRaceRealConn }

func (c *sessionRaceWSConn) WriteServerText([]byte) error {
	c.writes.Add(1)
	c.textWrites.Add(1)
	return nil
}
func (c *sessionRaceWSConn) WriteServerBinary([]byte) error { c.writes.Add(1); return nil }
