package api

import (
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	pushhandler "github.com/WuKongIM/WuKongIM/internal/pusher/handler"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestRevokedSessionExcludedFromOnlinePush(t *testing.T) {
	for _, kind := range []string{"RECV", "EVENT"} {
		for _, explicit := range []bool{false, true} {
			name := kind + "/all"
			if explicit {
				name = kind + "/explicit"
			}
			t.Run(name, func(t *testing.T) {
				f := newSessionRaceFixtureConfig(t, 1, false)
				old, healthy := sessionRaceConn(100), sessionRaceConn(200)
				healthy.ConnId++
				old.Revoked.Store(true)
				f.users.UpdateConn(old)
				f.users.UpdateConn(healthy)
				var frame wkproto.Frame = &wkproto.SendPacket{Framer: wkproto.Framer{NoPersist: true},
					ChannelID: old.Uid, ChannelType: wkproto.ChannelTypePerson, Payload: []byte("body")}
				if kind == "EVENT" {
					frame = &wkproto.EventPacket{}
				}
				event := &eventbus.Event{Type: eventbus.EventPushOnline, ToUid: old.Uid, Frame: frame,
					Conn: &eventbus.Conn{Uid: "sender", NodeId: 3, ConnId: 5}}
				if explicit {
					event.ToConns = []*eventbus.Conn{recoverySessionSnapshot(t, old), recoverySessionSnapshot(t, healthy)}
				}
				pushhandler.NewHandler().OnEvent(&eventbus.PushContext{EventType: eventbus.EventPushOnline, Events: []*eventbus.Event{event}})
				f.users.mu.Lock()
				defer f.users.mu.Unlock()
				if len(f.users.events) != 1 || f.users.events[0].Conn != healthy {
					t.Fatal("在线推送必须排除撤销会话，并保持有效会话原样接收")
				}
			})
		}
	}
}

func TestRevokedSessionQueuedWriteGate(t *testing.T) {
	for _, transport := range []string{"local", "remote", "jsonrpc"} {
		for _, kind := range []string{"RECV", "EVENT", "DISCONNECT", "SENDACK", "CONNACK"} {
			for _, mode := range []string{"event-revoked", "directory-revoked", "valid", "other-generation-revoked", "socket-revoked"} {
				if transport == "remote" && mode == "socket-revoked" {
					continue
				}
				t.Run(transport+"/"+kind+"/"+mode, func(t *testing.T) {
					f := newSessionRaceFixtureConfig(t, 1, false)
					db := installRecoverySessionDB(f)
					cluster := &recoveryWriteCluster{sessionRaceCluster: f.cluster}
					service.Cluster = cluster
					current := sessionRaceConn(100)
					current.ProtoVersion = wkproto.LatestVersion
					if transport == "remote" {
						current.NodeId = 2
					}
					if transport == "jsonrpc" {
						current.IsJsonRpc = true
						service.ConnManager = &sessionRaceConnManager{real: f.real, ws: true}
					}
					f.users.UpdateConn(current)
					f.real.SetContext(current)
					queued := recoverySessionSnapshot(t, current)
					switch mode {
					case "event-revoked":
						queued.Revoked.Store(true)
					case "directory-revoked":
						current.Revoked.Store(true)
					case "socket-revoked":
						socket := recoverySessionSnapshot(t, current)
						socket.Revoked.Store(true)
						f.real.SetContext(socket)
					case "other-generation-revoked":
						stale := sessionRaceConn(100)
						stale.NodeId = current.NodeId
						stale.Revoked.Store(true)
						f.users.UpdateConn(stale)
					}
					var frame wkproto.Frame
					switch kind {
					case "RECV":
						frame = &wkproto.RecvPacket{ChannelID: "sender", Payload: []byte("body")}
					case "EVENT":
						frame = &wkproto.EventPacket{}
					case "DISCONNECT":
						frame = &wkproto.DisconnectPacket{ReasonCode: wkproto.ReasonConnectKick}
					case "SENDACK":
						frame = &wkproto.SendackPacket{ClientSeq: 9, ReasonCode: wkproto.ReasonSystemError}
					case "CONNACK":
						frame = &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonAuthFail}
					}
					// 沿 RetryManager 相同的 ConnWrite 入队入口，撤销发生在异步消费前。
					eventbus.User.ConnWrite("original-request", queued, frame)
					f.users.mu.Lock()
					events := append([]*eventbus.Event(nil), f.users.events...)
					f.users.mu.Unlock()
					f.handler.OnEvent(&eventbus.UserContext{Uid: current.Uid, EventType: eventbus.EventConnWriteFrame, Events: events})
					blocked := (kind == "RECV" || kind == "EVENT") && mode != "valid" && mode != "other-generation-revoked"
					var want int32 = 1
					if blocked {
						want = 0
					}
					writes := f.real.writes.Load() + cluster.writes.Load()
					if writes != want {
						t.Fatalf("异步写出次数 %d，期望 %d；撤销只能拦用户可见推送", writes, want)
					}
					if db.reads.Load() != 0 {
						t.Fatal("写出撤销检查不得查询数据库")
					}
				})
			}
		}
	}
}

func TestRevokedSessionWriteRechecksAfterEncoding(t *testing.T) {
	for _, kind := range []string{"RECV", "EVENT"} {
		t.Run(kind, func(t *testing.T) {
			f := newSessionRaceFixtureConfig(t, 1, false)
			current := sessionRaceConn(100)
			current.ProtoVersion = wkproto.LatestVersion
			f.users.UpdateConn(current)
			f.real.SetContext(current)
			var frame wkproto.Frame = &wkproto.RecvPacket{Payload: []byte("body")}
			if kind == "EVENT" {
				frame = &wkproto.EventPacket{}
			}
			previous := eventbus.Proto
			defer func() { eventbus.Proto = previous }()
			eventbus.Proto = recoveryWriteProtocol{Protocol: previous, afterEncode: func() { current.Revoked.Store(true) }}
			f.handler.OnEvent(&eventbus.UserContext{Uid: current.Uid, EventType: eventbus.EventConnWriteFrame,
				Events: []*eventbus.Event{{Conn: recoverySessionSnapshot(t, current), Frame: frame}}})
			if f.real.writes.Load() != 0 {
				t.Fatal("编码期间撤销后，最终 socket 写出仍泄漏了用户可见内容")
			}
		})
	}
}

type recoveryWriteCluster struct {
	*sessionRaceCluster
	writes atomic.Int32
}

func (c *recoveryWriteCluster) Send(uint64, *proto.Message) error {
	c.writes.Add(1)
	return nil
}

type recoveryWriteProtocol struct {
	wkproto.Protocol
	afterEncode func()
}

func (p recoveryWriteProtocol) EncodeFrame(frame wkproto.Frame, version uint8) ([]byte, error) {
	data, err := p.Protocol.EncodeFrame(frame, version)
	p.afterEncode()
	return data, err
}
