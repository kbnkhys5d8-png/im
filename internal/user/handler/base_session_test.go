package handler

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/common"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type sessionForwardCluster struct{ icluster.ICluster }

func (sessionForwardCluster) GetSlotId(string) uint32    { return 1 }
func (sessionForwardCluster) SlotLeaderId(uint32) uint64 { return 1 }

type sessionForwardUser struct {
	eventbus.IUser
	current     *eventbus.Conn
	events      []*eventbus.Event
	invalidated []*eventbus.Conn
}

func (u *sessionForwardUser) ConnById(string, uint64, int64) *eventbus.Conn { return u.current }
func (u *sessionForwardUser) AddEvent(_ string, event *eventbus.Event) {
	u.events = append(u.events, event)
}
func (u *sessionForwardUser) Advance(string)                 {}
func (u *sessionForwardUser) UpdateConn(conn *eventbus.Conn) { u.current = conn }
func (u *sessionForwardUser) InvalidateConnRecovery(conn *eventbus.Conn) bool {
	u.invalidated = append(u.invalidated, conn)
	return true
}

func TestForwardUserEventRejectsStaleSession(t *testing.T) {
	previousOptions, previousCluster, previousUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = previousOptions, previousCluster, previousUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = sessionForwardCluster{}
	for _, eventType := range []eventbus.EventType{eventbus.EventConnWriteFrame, eventbus.EventConnClose, eventbus.EventConnRemove, eventbus.EventConnLeaderRemove} {
		t.Run(eventType.String(), func(t *testing.T) {
			u := &sessionForwardUser{current: &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 4, Auth: true}}
			eventbus.RegisterUser(u)
			req := &forwardUserEventReq{uid: "user", fromNode: 2, events: []*eventbus.Event{{
				Type: eventType, Conn: &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, Auth: true},
				Frame: &wkproto.PingPacket{},
			}}}
			data, err := req.encode()
			if err != nil {
				t.Fatal(err)
			}
			h := &Handler{Log: wklog.NewWKLog("session-test")}
			h.onForwardUserEvent(&proto.Message{Content: data})
			if len(u.events) != 0 {
				t.Fatal("旧会话写入或关闭事件不应替换成新会话后入队")
			}
		})
	}
}

func TestForwardUserEventPreservesCurrentSessionAndHandshake(t *testing.T) {
	previousOptions, previousCluster, previousUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = previousOptions, previousCluster, previousUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = sessionForwardCluster{}
	tests := []struct {
		name    string
		typ     eventbus.EventType
		current *eventbus.Conn
	}{
		{name: "同会话写入", typ: eventbus.EventConnWriteFrame, current: &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "session", Auth: true}},
		{name: "成功握手回执", typ: eventbus.EventConnack, current: &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "session"}},
		{name: "拒绝握手回执", typ: eventbus.EventConnack},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := &sessionForwardUser{current: tt.current}
			eventbus.RegisterUser(u)
			target := &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "session", Auth: true}
			if tt.typ == eventbus.EventConnack {
				target.AesKey = []byte("handshake-key")
			}
			req := &forwardUserEventReq{uid: "user", fromNode: 2, events: []*eventbus.Event{{Type: tt.typ, Conn: target, Frame: &wkproto.ConnackPacket{}}}}
			data, err := req.encode()
			if err != nil {
				t.Fatal(err)
			}
			h := &Handler{Log: wklog.NewWKLog("session-test")}
			h.onForwardUserEvent(&proto.Message{Content: data})
			if len(u.events) != 1 {
				t.Fatal("当前会话及握手事件应保持原入队路径")
			}
			if tt.current != nil && u.events[0].Conn != tt.current {
				t.Fatal("未保持原连接替换行为")
			}
		})
	}
}

func TestForwardUserEventRejectsReusedIDInSameSecond(t *testing.T) {
	previousOptions, previousCluster, previousUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = previousOptions, previousCluster, previousUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = sessionForwardCluster{}
	u := &sessionForwardUser{current: &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "new", Auth: true, IsJsonRpc: true}}
	eventbus.RegisterUser(u)
	req := &forwardUserEventReq{uid: "user", fromNode: 2, events: []*eventbus.Event{{
		Type: eventbus.EventConnWriteFrame, Conn: &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "old", Auth: true, IsJsonRpc: true},
		Frame: &wkproto.PingPacket{},
	}}}
	data, err := req.encode()
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{Log: wklog.NewWKLog("session-test")}
	h.onForwardUserEvent(&proto.Message{Content: data})
	if len(u.events) != 0 {
		t.Fatal("无密钥且同秒复用连接 ID 的旧事件不能投递给新会话")
	}
}

type sessionHandshakeConn struct {
	wknet.Conn
	ctx interface{}
}

func (c *sessionHandshakeConn) Context() interface{}     { return c.ctx }
func (c *sessionHandshakeConn) SetContext(v interface{}) { c.ctx = v }
func (c *sessionHandshakeConn) SetMaxIdle(time.Duration) {}
func (c *sessionHandshakeConn) Fd() wknet.NetFd          { return wknet.NetFd{} }

type sessionHandshakeManager struct {
	service.IConnManager
	conn wknet.Conn
}

func (m sessionHandshakeManager) GetConn(int64) wknet.Conn   { return m.conn }
func (m sessionHandshakeManager) GetConnByFd(int) wknet.Conn { return m.conn }

func TestConnackSessionValidationKeepsHandshakeWrites(t *testing.T) {
	previousOptions, previousManager, previousUser := options.G, service.ConnManager, eventbus.User
	t.Cleanup(func() { options.G, service.ConnManager, eventbus.User = previousOptions, previousManager, previousUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	for _, tt := range []struct {
		name    string
		reason  wkproto.ReasonCode
		session string
	}{
		{name: "新成功", reason: wkproto.ReasonSuccess, session: "session"},
		{name: "新拒绝", reason: wkproto.ReasonAuthFail, session: "session"},
		{name: "旧成功", reason: wkproto.ReasonSuccess},
		{name: "旧拒绝", reason: wkproto.ReasonAuthFail},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reason := tt.reason
			initial := &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: tt.session, ProtoVersion: 5}
			target := &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: tt.session, ProtoVersion: 5}
			if reason == wkproto.ReasonSuccess {
				target.Auth, target.ProtoVersion, target.AesKey = true, 4, []byte("negotiated-key")
			}
			real := &sessionHandshakeConn{ctx: initial}
			service.ConnManager = sessionHandshakeManager{conn: real}
			u := &sessionForwardUser{current: initial}
			eventbus.RegisterUser(u)
			h := &Handler{Log: wklog.NewWKLog("session-test")}
			h.connack(&eventbus.UserContext{Events: []*eventbus.Event{{Conn: target, Frame: &wkproto.ConnackPacket{ReasonCode: reason}}}})
			if len(u.events) != 1 || u.events[0].Type != eventbus.EventConnWriteFrame {
				t.Fatal("握手成功和拒绝仍应产生原 CONNACK 写入事件")
			}
			if got, err := common.CheckConnValidAndGetRealConn(u.events[0].Conn); err != nil || got != real {
				t.Fatalf("会话校验不应阻止原握手回执：%v", err)
			}
		})
	}
}

func TestForwardConnackRejectsReusedSession(t *testing.T) {
	previousOptions, previousCluster, previousUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = previousOptions, previousCluster, previousUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = sessionForwardCluster{}
	u := &sessionForwardUser{current: &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, SessionId: "new"}}
	eventbus.RegisterUser(u)
	req := &forwardUserEventReq{uid: "user", fromNode: 2, events: []*eventbus.Event{{
		Type: eventbus.EventConnack, Conn: &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, SessionId: "old", Auth: true},
		Frame: &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonSuccess},
	}}}
	data, err := req.encode()
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{Log: wklog.NewWKLog("session-test")}
	h.onForwardUserEvent(&proto.Message{Content: data})
	if len(u.events) != 0 {
		t.Fatal("旧 CONNACK 不得替换成新会话后入队")
	}
}

func TestConnackRejectsReusedRealSocketWithoutDirectory(t *testing.T) {
	previousOptions, previousManager, previousUser := options.G, service.ConnManager, eventbus.User
	t.Cleanup(func() { options.G, service.ConnManager, eventbus.User = previousOptions, previousManager, previousUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	for _, reason := range []wkproto.ReasonCode{wkproto.ReasonSuccess, wkproto.ReasonAuthFail} {
		t.Run(reason.String(), func(t *testing.T) {
			current := &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, SessionId: "new"}
			real := &sessionHandshakeConn{ctx: current}
			service.ConnManager = sessionHandshakeManager{conn: real}
			u := &sessionForwardUser{}
			eventbus.RegisterUser(u)
			target := &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, SessionId: "old", Auth: true}
			h := &Handler{Log: wklog.NewWKLog("session-test")}
			h.connack(&eventbus.UserContext{Events: []*eventbus.Event{{Conn: target, Frame: &wkproto.ConnackPacket{ReasonCode: reason}}}})
			if real.ctx != current || u.current != nil || len(u.events) != 0 {
				t.Fatal("逻辑目录尚无连接时，迟到握手回执也不能覆盖真实新 socket 或写入")
			}
		})
	}
}

func TestForwardConnectPreservesNewSessionAndConnack(t *testing.T) {
	previousOptions, previousCluster := options.G, service.Cluster
	previousUser, previousManager := eventbus.User, service.ConnManager
	t.Cleanup(func() {
		options.G, service.Cluster = previousOptions, previousCluster
		eventbus.User, service.ConnManager = previousUser, previousManager
	})
	for _, tt := range []struct {
		name   string
		uptime uint64
	}{
		{name: "重启后复用连接号", uptime: 20},
		{name: "同秒复用连接号", uptime: 10},
	} {
		t.Run(tt.name, func(t *testing.T) {
			options.G = options.New()
			options.G.Cluster.NodeId = 1
			service.Cluster = sessionForwardCluster{}
			old := &eventbus.Conn{
				Uid: "user", NodeId: 2, ConnId: 1, SessionId: "before-owner-restart",
				Uptime: 10, Auth: true, ProtoVersion: 5,
			}
			fresh := &eventbus.Conn{
				Uid: "user", NodeId: 2, ConnId: 1, SessionId: "after-owner-restart",
				Uptime: tt.uptime, ProtoVersion: 5,
			}
			u := &sessionForwardUser{current: old}
			eventbus.RegisterUser(u)
			req := &forwardUserEventReq{uid: "user", fromNode: 2, events: []*eventbus.Event{{
				Type: eventbus.EventConnect, Conn: fresh, SourceNodeId: 2,
				Frame: &wkproto.ConnectPacket{Version: 5, UID: "user", DeviceID: "device"},
			}}}
			data, err := req.encode()
			if err != nil {
				t.Fatal(err)
			}
			h := &Handler{Log: wklog.NewWKLog("connect-session-test")}
			h.onForwardUserEvent(&proto.Message{Content: data})
			if len(u.events) != 1 {
				t.Fatalf("CONNECT 事件数 = %d，期望 1", len(u.events))
			}
			forwarded := u.events[0].Conn
			if !forwarded.SameSession(fresh) || forwarded.Auth {
				t.Errorf("新 CONNECT 被旧目录覆盖：session=%q auth=%v", forwarded.SessionId, forwarded.Auth)
			}

			// 认证只更新认证信息，不重新生成会话代次；真实回执处理仍须写给新 socket。
			forwarded.Auth = true
			real := &sessionHandshakeConn{ctx: fresh}
			service.ConnManager = sessionHandshakeManager{conn: real}
			options.G.Cluster.NodeId = 2
			u.events = nil
			h.connack(&eventbus.UserContext{Events: []*eventbus.Event{{
				Conn: forwarded, Frame: &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonSuccess},
			}}})
			if len(u.events) != 1 || u.events[0].Type != eventbus.EventConnWriteFrame {
				t.Fatalf("新 socket 没有收到成功 CONNACK，写事件数 = %d", len(u.events))
			}
			if got, err := common.CheckConnValidAndGetRealConn(u.events[0].Conn); err != nil || got != real {
				t.Fatalf("新握手回执未通过实际会话校验：%v", err)
			}
		})
	}
}

func TestForwardConnectKeepsExistingSessionBehavior(t *testing.T) {
	previousOptions, previousCluster, previousUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = previousOptions, previousCluster, previousUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = sessionForwardCluster{}
	for _, tt := range []struct {
		name    string
		session string
	}{
		{name: "同会话", session: "current-session"},
		{name: "旧编码空会话"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			current := &eventbus.Conn{Uid: "user", NodeId: 2, ConnId: 1, SessionId: tt.session, ProtoVersion: 5}
			u := &sessionForwardUser{current: current}
			eventbus.RegisterUser(u)
			req := &forwardUserEventReq{uid: "user", fromNode: 2, events: []*eventbus.Event{{
				Type:  eventbus.EventConnect,
				Conn:  &eventbus.Conn{Uid: "user", NodeId: 2, ConnId: 1, SessionId: tt.session, ProtoVersion: 5},
				Frame: &wkproto.ConnectPacket{Version: 5, UID: "user"},
			}}}
			data, err := req.encode()
			if err != nil {
				t.Fatal(err)
			}
			h := &Handler{Log: wklog.NewWKLog("connect-session-test")}
			h.onForwardUserEvent(&proto.Message{Content: data})
			if len(u.events) != 1 || u.events[0].Conn != current {
				t.Fatal("同会话与旧编码 CONNECT 应保持原连接替换行为")
			}
		})
	}
}

func TestForwardUnknownAuthenticatedSessionInvalidatesRecovery(t *testing.T) {
	previousOptions, previousCluster, previousUser := options.G, service.Cluster, eventbus.User
	t.Cleanup(func() { options.G, service.Cluster, eventbus.User = previousOptions, previousCluster, previousUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = sessionForwardCluster{}
	for _, tt := range []struct {
		name            string
		currentSession  string
		incomingSession string
		missing         bool
		auth            bool
		internal        bool
		invalidate      bool
		notLeader       bool
	}{
		{name: "目录缺失", incomingSession: "new", missing: true, auth: true, invalidate: true},
		{name: "目录保留旧会话", currentSession: "old", incomingSession: "new", auth: true, invalidate: true},
		{name: "已知会话", currentSession: "current", incomingSession: "current", auth: true},
		{name: "未认证不触发", currentSession: "old", incomingSession: "new"},
		{name: "内部连接不触发", currentSession: "old", incomingSession: "new", auth: true, internal: true},
		{name: "空会话不触发", currentSession: "old", auth: true},
		{name: "非用户领导不触发", currentSession: "old", incomingSession: "new", auth: true, notLeader: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			options.G.Cluster.NodeId = 1
			if tt.notLeader {
				options.G.Cluster.NodeId = 2
			}
			u := &sessionForwardUser{}
			if !tt.missing {
				u.current = &eventbus.Conn{
					Uid: "user", NodeId: 2, ConnId: 1, SessionId: tt.currentSession,
					Auth: true, ProtoVersion: 5,
				}
			}
			before := u.current
			eventbus.RegisterUser(u)
			incoming := &eventbus.Conn{
				Uid: "user", NodeId: 2, ConnId: 1, SessionId: tt.incomingSession,
				Auth: tt.auth, Internal: tt.internal, ProtoVersion: 5,
			}
			req := &forwardUserEventReq{uid: "user", fromNode: 2, events: []*eventbus.Event{{
				Type: eventbus.EventOnSend, Conn: incoming, Frame: &wkproto.PingPacket{}, SourceNodeId: 2,
			}}}
			data, err := req.encode()
			if err != nil {
				t.Fatal(err)
			}
			h := &Handler{Log: wklog.NewWKLog("unknown-session-test")}
			h.onForwardUserEvent(&proto.Message{Content: data})
			if tt.notLeader {
				if len(u.events) != 0 || len(u.invalidated) != 0 || u.current != before {
					t.Fatal("非用户领导节点不能处理发送事件或改变恢复缓存")
				}
				return
			}
			if len(u.events) != 1 {
				t.Fatalf("发送事件数 = %d，期望 1", len(u.events))
			}
			if tt.invalidate {
				if len(u.invalidated) != 1 || !u.invalidated[0].SameSession(incoming) {
					t.Error("未知认证会话必须标记恢复缓存失效")
				}
				if !u.events[0].Conn.SameSession(incoming) {
					t.Error("未知认证会话不得替换成旧目录指针")
				}
			} else {
				if len(u.invalidated) != 0 || u.events[0].Conn != before {
					t.Error("已知或不满足恢复条件的会话应保持原替换行为")
				}
			}
			if u.current != before {
				t.Fatal("不能根据转发事件的 Auth 标记直接登记新连接")
			}
		})
	}
}
