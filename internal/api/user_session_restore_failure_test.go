package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestSessionRestoreFailureRejectsFrameAndClosesExactSource(t *testing.T) {
	for _, fault := range []string{"source", "device", "channel", "leader"} {
		for _, kind := range []string{"SEND", "RECVACK", "PING"} {
			t.Run(fault+"/"+kind, func(t *testing.T) {
				fixture := newSessionRaceFixture(t)
				conn := sessionRaceConn(100)
				conn.NodeId = 3
				fixture.cluster.peers = map[uint64][]*eventbus.Conn{3: {conn}}
				if fault == "source" {
					fixture.cluster.requestErr = errors.New("模拟源节点暂不可达")
				} else if fault == "leader" {
					fixture.db.channelGate = func() { fixture.cluster.leader.Store(2) }
				} else {
					service.Store = store.New(store.NewOptions(store.WithDB(&restoreFailureDB{DB: fixture.db, fault: fault}), store.WithSlot(fixture.cluster)))
				}
				var frame wkproto.Frame = &wkproto.PingPacket{}
				switch kind {
				case "SEND":
					frame = &wkproto.SendPacket{Framer: wkproto.Framer{RedDot: true}, ClientSeq: 17, ClientMsgNo: "retry-original", ChannelID: "receiver", Payload: []byte("body")}
				case "RECVACK":
					frame = &wkproto.RecvackPacket{MessageID: 18}
				}
				fixture.handler.OnEvent(&eventbus.UserContext{Uid: conn.Uid, EventType: eventbus.EventOnSend, AddConn: fixture.users.UpdateConn,
					Events: []*eventbus.Event{{Type: eventbus.EventOnSend, Conn: conn, Frame: frame, MessageId: 19, ReqId: "original-request"}},
				})
				closed := restoreFailureClose(t, fixture)
				if !closed.Conn.SameSession(conn) || closed.ReqId != "original-request" {
					t.Fatal("恢复失败关闭了错误代际或丢失原请求标识")
				}
				if kind == "SEND" {
					ack, ok := closed.Frame.(*wkproto.SendackPacket)
					if !ok || ack.ReasonCode != wkproto.ReasonSystemError || ack.ClientSeq != 17 || ack.ClientMsgNo != "retry-original" || ack.MessageID != 19 || !ack.RedDot {
						t.Fatal("恢复失败没有保留旧协议发送失败回执及幂等标识")
					}
				} else if closed.Frame != nil {
					t.Fatal("临时恢复失败不能伪装成踢号，也不能发送成功 PONG")
				}
				if fixture.users.ConnById(conn.Uid, conn.NodeId, conn.ConnId) != nil {
					t.Fatal("查询失败时不得绕过校验恢复会话")
				}
			})
		}
	}
}

func TestSessionRestoreFailureWritesLegacySendackBeforeClose(t *testing.T) {
	for _, version := range []uint8{1, 3, wkproto.LatestVersion} {
		for _, failWrite := range []bool{false, true} {
			t.Run(fmt.Sprintf("v%d/write-failure=%t", version, failWrite), func(t *testing.T) {
				fixture := newSessionRaceFixture(t)
				conn := sessionRaceConn(100)
				conn.ProtoVersion = version
				fixture.real.SetContext(conn)
				fixture.users.UpdateConn(conn)
				if failWrite {
					fixture.real.writeErr = errors.New("模拟发送失败")
				}
				fixture.handler.OnEvent(&eventbus.UserContext{Uid: conn.Uid, EventType: eventbus.EventConnClose, Events: []*eventbus.Event{{Conn: conn,
					Frame: &wkproto.SendackPacket{ClientSeq: 17, ClientMsgNo: "retry-original", ReasonCode: wkproto.ReasonSystemError},
				}}})
				if !fixture.real.closed.Load() {
					t.Fatal("回执写入异常也必须关闭原精确会话，不能留下假在线")
				}
				if failWrite {
					return
				}
				data := fixture.real.lastData.Load()
				if data == nil || fixture.real.flushes.Load() != 1 {
					t.Fatal("关闭前未尝试写入及刷新旧协议失败回执")
				}
				frame, _, err := eventbus.Proto.DecodeFrame(*data, version)
				if err != nil {
					t.Fatal(err)
				}
				ack, ok := frame.(*wkproto.SendackPacket)
				// 旧二进制协议按 ClientSeq 关联，不新增线上不存在的 ClientMsgNo 字段。
				if !ok || ack.ReasonCode != wkproto.ReasonSystemError || ack.ClientSeq != 17 {
					t.Fatal("旧协议无法识别原发送请求的失败回执")
				}
			})
		}
	}
}

func TestSessionRestoreFailureKeepsJSONRPCRequestID(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	conn := sessionRaceConn(100)
	conn.IsJsonRpc = true
	fixture.real.SetContext(conn)
	fixture.users.UpdateConn(conn)
	fixture.handler.OnEvent(&eventbus.UserContext{Uid: conn.Uid, EventType: eventbus.EventConnClose, Events: []*eventbus.Event{{Conn: conn, ReqId: "original-request",
		Frame: &wkproto.SendackPacket{ReasonCode: wkproto.ReasonSystemError},
	}}})
	data := fixture.real.lastData.Load()
	if data == nil || !fixture.real.closed.Load() {
		t.Fatal("JSON-RPC 未写失败回执或未关闭原会话")
	}
	var response struct {
		ID     string `json:"id"`
		Result struct {
			ReasonCode int `json:"reasonCode"`
		} `json:"result"`
	}
	if err := json.Unmarshal(*data, &response); err != nil {
		t.Fatal(err)
	}
	if response.ID != "original-request" || response.Result.ReasonCode != int(wkproto.ReasonSystemError) {
		t.Fatal("JSON-RPC 原请求标识或失败原因丢失")
	}
}

func TestSessionRestoreFailureCannotCloseReplacement(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	old := sessionRaceConn(100)
	service.Store = store.New(store.NewOptions(store.WithDB(&restoreFailureDB{DB: fixture.db, fault: "device"}), store.WithSlot(fixture.cluster)))
	fixture.handler.OnEvent(&eventbus.UserContext{Uid: old.Uid, EventType: eventbus.EventOnSend, AddConn: fixture.users.UpdateConn,
		Events: []*eventbus.Event{{Type: eventbus.EventOnSend, Conn: old, Frame: &wkproto.PingPacket{}}},
	})
	closed := restoreFailureClose(t, fixture)
	// 模拟关闭事件在途时，新连接复用了同秒、同连接号，旧恢复失败不能误关它。
	current := sessionRaceConn(100)
	fixture.real.SetContext(current)
	fixture.users.UpdateConn(current)
	fixture.handler.OnEvent(&eventbus.UserContext{Uid: current.Uid, EventType: eventbus.EventConnClose, Events: []*eventbus.Event{closed}})
	if fixture.real.closed.Load() || fixture.users.ConnById(current.Uid, current.NodeId, current.ConnId) != current {
		t.Fatal("晚到的恢复失败关闭误伤新连接")
	}
}

func TestSessionRestoreFailureClosesEveryRemoteInBatch(t *testing.T) {
	fixture := newSessionRaceFixture(t)
	var mu sync.Mutex
	var forwarded []*eventbus.Event
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		decoder := wkproto.NewDecoder(data)
		if _, err = decoder.String(); err != nil {
			t.Error(err)
			return
		}
		if _, err = decoder.Uint64(); err != nil {
			t.Error(err)
			return
		}
		body, err := decoder.BinaryAll()
		if err != nil {
			t.Error(err)
			return
		}
		var batch eventbus.EventBatch
		if err = batch.Decode(body); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		forwarded = append(forwarded, batch...)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	fixture.cluster.forwardURL = server.URL
	var events []*eventbus.Event
	for _, node := range []uint64{2, 3} {
		conn := sessionRaceConn(100)
		conn.NodeId = node
		fixture.users.UpdateConn(conn)
		events = append(events, &eventbus.Event{Type: eventbus.EventConnClose, Conn: conn,
			Frame: &wkproto.SendackPacket{ClientSeq: node, ReasonCode: wkproto.ReasonSystemError}})
	}
	fixture.handler.OnEvent(&eventbus.UserContext{Uid: events[0].Conn.Uid, EventType: eventbus.EventConnClose, Events: events})
	mu.Lock()
	defer mu.Unlock()
	if len(forwarded) != len(events) {
		t.Fatalf("批量恢复失败只关闭了部分源连接: got=%d want=%d", len(forwarded), len(events))
	}
	for index, event := range forwarded {
		if !event.Conn.SameSession(events[index].Conn) || event.Frame.(*wkproto.SendackPacket).ReasonCode != wkproto.ReasonSystemError {
			t.Fatal("远端关闭丢失原会话或发送失败回执")
		}
	}
}

func restoreFailureClose(t *testing.T, fixture *sessionRaceFixture) *eventbus.Event {
	t.Helper()
	fixture.users.mu.Lock()
	defer fixture.users.mu.Unlock()
	for _, event := range fixture.users.events {
		if event.Type == eventbus.EventConnClose {
			return event
		}
	}
	t.Fatal("恢复失败被静默跳过，未明确关闭源会话")
	return nil
}

type restoreFailureDB struct {
	wkdb.DB
	fault string
}

func (d *restoreFailureDB) GetDevice(uid string, flag uint64) (wkdb.Device, error) {
	if d.fault == "device" {
		return wkdb.Device{}, errors.New("模拟设备查询暂时失败")
	}
	return d.DB.GetDevice(uid, flag)
}

func (d *restoreFailureDB) GetChannel(id string, kind uint8) (wkdb.ChannelInfo, error) {
	if d.fault == "channel" {
		return wkdb.ChannelInfo{}, errors.New("模拟频道查询暂时失败")
	}
	return d.DB.GetChannel(id, kind)
}
