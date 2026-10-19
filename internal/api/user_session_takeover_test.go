package api

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type takeoverPlugin struct{ service.IPluginManager }

func (*takeoverPlugin) Plugins(...types.PluginMethod) []types.Plugin { return nil }

type takeoverChannel struct{ sends atomic.Int32 }

func (c *takeoverChannel) AddEvent(string, uint8, *eventbus.Event) { c.sends.Add(1) }
func (*takeoverChannel) Advance(string, uint8)                     {}

type takeoverRetry struct {
	service.RetryMgr
	acks atomic.Int32
}

func (*takeoverRetry) RetryMessage(uint64, int64, int64) *types.RetryMessage { return nil }
func (r *takeoverRetry) RemoveRetry(uint64, int64, int64) error              { r.acks.Add(1); return nil }

func TestNewLeaderRestoresValidatedForwardedSession(t *testing.T) {
	for _, kind := range []string{"SEND", "RECVACK", "PING"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newSessionRaceFixture(t)
			oldPlugin, oldChannel, oldRetry := service.PluginManager, eventbus.Channel, service.RetryManager
			channel, retry := &takeoverChannel{}, &takeoverRetry{}
			service.PluginManager, service.RetryManager = &takeoverPlugin{}, retry
			eventbus.RegisterChannel(channel)
			t.Cleanup(func() {
				fixture.pool.Stop()
				service.PluginManager, eventbus.Channel, service.RetryManager = oldPlugin, oldChannel, oldRetry
			})
			conn := sessionRaceConn(100)
			conn.NodeId = 3
			fixture.cluster.peers = map[uint64][]*eventbus.Conn{3: {conn}}
			var frame wkproto.Frame = &wkproto.PingPacket{}
			switch kind {
			case "SEND":
				frame = &wkproto.SendPacket{ChannelID: "receiver", ChannelType: wkproto.ChannelTypePerson, Payload: []byte(`{"type":1,"content":"body"}`)}
			case "RECVACK":
				frame = &wkproto.RecvackPacket{MessageID: 12}
			}
			done := make(chan struct{})
			fixture.observer.afterOnSend = func() { close(done) }
			fixture.handler.OnMessage(sessionRaceForward(t, &eventbus.Event{Type: eventbus.EventOnSend, Conn: conn, Frame: frame, SourceNodeId: 3}, 3))
			waitSessionRace(t, done)
			current := fixture.users.ConnById(conn.Uid, conn.NodeId, conn.ConnId)
			if !current.SameSession(conn) {
				t.Fatal("新 leader 空索引未恢复合法源会话，迁移后帧被静默丢弃")
			}
			if kind == "SEND" && channel.sends.Load() != 1 {
				t.Fatal("合法 SEND 未进入原处理路径")
			}
			if kind == "RECVACK" && retry.acks.Load() != 1 {
				t.Fatal("合法 RECVACK 未进入原处理路径")
			}
			if kind == "PING" {
				fixture.users.mu.Lock()
				defer fixture.users.mu.Unlock()
				var pongs int
				for _, e := range fixture.users.events {
					if _, ok := e.Frame.(*wkproto.PongPacket); ok {
						pongs++
					}
				}
				if pongs != 1 {
					t.Fatal("合法 PING 未回复")
				}
			}
		})
	}
}

func TestNewLeaderDoesNotRestoreStaleOrBannedSession(t *testing.T) {
	for _, mode := range []string{"stale-token", "banned", "replaced-at-source", "not-authed"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newSessionRaceFixture(t)
			conn := sessionRaceConn(100)
			conn.NodeId = 3
			source := conn
			switch mode {
			case "stale-token":
				conn.AuthTokenHash = eventbus.TokenFingerprint("stale")
			case "banned":
				fixture.db.ban.Store(true)
			case "replaced-at-source":
				source = sessionRaceConn(100)
				source.NodeId = 3
			case "not-authed":
				conn.Auth = false
			}
			fixture.cluster.peers = map[uint64][]*eventbus.Conn{3: {source}}
			done := make(chan struct{})
			fixture.observer.afterOnSend = func() { close(done) }
			fixture.handler.OnMessage(sessionRaceForward(t, &eventbus.Event{Type: eventbus.EventOnSend, Conn: conn, Frame: &wkproto.PingPacket{}, SourceNodeId: 3}, 3))
			waitSessionRace(t, done)
			if fixture.users.ConnById(conn.Uid, conn.NodeId, conn.ConnId) != nil {
				t.Fatal("新 leader 盲目重加了过时、封禁或未认证源会话")
			}
		})
	}
}

func TestNewLeaderRevokesUnindexedSourceConnections(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	old, current := sessionRaceConn(100), sessionRaceConn(100)
	old.NodeId, current.NodeId = 3, 4
	current.AuthTokenHash = eventbus.TokenFingerprint("new-token")
	fixture.cluster.peers = map[uint64][]*eventbus.Conn{3: {old}, 4: {current}}
	if response := fixture.update("new-token", 42); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	var oldClosed bool
	for _, conn := range fixture.cluster.revoked {
		if conn.SameSession(old) {
			oldClosed = true
		}
		if conn.SameSession(current) {
			t.Fatal("撤销按源节点查询时误踢当前 Token 的新会话")
		}
	}
	if !oldClosed {
		t.Fatal("新 leader 索引为空时漏掉源 C 的旧连接")
	}
}

func TestNewLeaderSameTokenRefreshKeepsUnindexedSession(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	conn := sessionRaceConn(100)
	conn.NodeId = 3
	fixture.cluster.peers = map[uint64][]*eventbus.Conn{3: {conn}}
	if response := fixture.update("old-token", 42); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	if len(fixture.cluster.revoked) != 0 {
		t.Fatal("接管后同 Token 刷新踢掉了未索引的合法源会话")
	}
}

func TestNewLeaderCannotConfirmRevokeWhenSourceIsUnreachable(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	conn := sessionRaceConn(100)
	conn.NodeId = 3
	fixture.cluster.peers = map[uint64][]*eventbus.Conn{3: {conn}}
	fixture.cluster.requestErr = fmt.Errorf("源节点不可达")
	if response := fixture.update("new-token", 42); response.Code == http.StatusOK {
		t.Fatal("新 leader 未定位源会话就确认撤销成功")
	}
}

func TestNewLeaderRevokesUnindexedPreAuthSession(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	conn := sessionRaceConn(100)
	conn.NodeId, conn.Auth = 3, false
	fixture.cluster.peers = map[uint64][]*eventbus.Conn{3: {conn}}
	if response := fixture.update("new-token", 42); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	if len(fixture.cluster.revoked) != 1 || !fixture.cluster.revoked[0].SameSession(conn) {
		t.Fatal("接管后漏掉源节点尚未收到 Connack 的旧 Token 会话")
	}
}
