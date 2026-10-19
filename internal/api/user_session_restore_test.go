package api

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestLeaderTakeoverReplacesStaleGenerationIndex(t *testing.T) {
	for _, state := range []string{"new-source", "old-source-event", "token-revoked", "user-banned"} {
		t.Run(state, func(t *testing.T) {
			fixture := newSessionRaceFixture(t)
			oldPlugins, oldRetry, oldChannel := service.PluginManager, service.RetryManager, eventbus.Channel
			retry, channel := &restoreRetryProbe{}, &restoreChannelProbe{}
			service.PluginManager, service.RetryManager = &restorePluginProbe{}, retry
			eventbus.RegisterChannel(channel)
			t.Cleanup(func() {
				service.PluginManager, service.RetryManager, eventbus.Channel = oldPlugins, oldRetry, oldChannel
			})

			// 同一秒、相同连接号，仅代际不同；源节点的物理会话是最终恢复依据。
			stale, current := sessionRaceConn(100), sessionRaceConn(100)
			stale.NodeId, current.NodeId = 2, 2
			fixture.cluster.peers = map[uint64][]*eventbus.Conn{2: {current}}
			incoming, indexed := current, stale
			if state == "old-source-event" {
				incoming, indexed = stale, current
			}
			fixture.users.UpdateConn(indexed)
			if state == "token-revoked" {
				fixture.db.device.Token = "new-token"
			}
			fixture.db.ban.Store(state == "user-banned")
			frames := []wkproto.Frame{
				&wkproto.SendPacket{ChannelID: "recipient", ChannelType: wkproto.ChannelTypePerson, Payload: []byte(`{"type":1,"content":"test"}`)},
				&wkproto.RecvackPacket{MessageID: 1},
				&wkproto.PingPacket{},
			}
			for _, frame := range frames {
				fixture.handler.OnEvent(&eventbus.UserContext{Uid: current.Uid, EventType: eventbus.EventOnSend,
					AddConn: fixture.users.UpdateConn, Events: []*eventbus.Event{{Type: eventbus.EventOnSend, Conn: incoming, Frame: frame}},
				})
			}
			var pongs int
			fixture.users.mu.Lock()
			for _, event := range fixture.users.events {
				if _, ok := event.Frame.(*wkproto.PongPacket); ok {
					pongs++
				}
			}
			fixture.users.mu.Unlock()
			expected := 0
			if state == "new-source" {
				expected = 1
				indexed = current
			}
			if channel.sent != expected || retry.removed != expected || pongs != expected {
				t.Fatalf("接管处理不符: send=%d ack=%d pong=%d expected=%d", channel.sent, retry.removed, pongs, expected)
			}
			if got := fixture.users.ConnById(current.Uid, current.NodeId, current.ConnId); got != indexed {
				t.Fatal("接管恢复覆盖了不应修改的会话或未恢复合法新会话")
			}
		})
	}
}

type restorePluginProbe struct{ service.IPluginManager }

func (*restorePluginProbe) Plugins(...types.PluginMethod) []types.Plugin { return nil }

type restoreRetryProbe struct {
	service.RetryMgr
	removed int
}

func (*restoreRetryProbe) RetryMessage(uint64, int64, int64) *types.RetryMessage { return nil }
func (r *restoreRetryProbe) RemoveRetry(uint64, int64, int64) error              { r.removed++; return nil }

type restoreChannelProbe struct{ sent int }

func (c *restoreChannelProbe) AddEvent(string, uint8, *eventbus.Event) { c.sent++ }
func (*restoreChannelProbe) Advance(string, uint8)                     {}
