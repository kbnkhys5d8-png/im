package common

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
)

type sessionTestRealConn struct {
	wknet.Conn
	ctx interface{}
}

func TestCheckConnValidRejectsSessionWithoutRealContext(t *testing.T) {
	previous := service.ConnManager
	t.Cleanup(func() { service.ConnManager = previous })
	for _, ctx := range []interface{}{nil, (*eventbus.Conn)(nil), "unrecognized-context"} {
		service.ConnManager = sessionTestConnManager{conn: &sessionTestRealConn{ctx: ctx}}
		target := &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, SessionId: "old-session"}
		if got, err := CheckConnValidAndGetRealConn(target); err == nil || got != nil {
			t.Fatal("带会话标识的旧目标不能命中尚无连接上下文的新 socket")
		}
	}
}

func (c *sessionTestRealConn) Context() interface{} { return c.ctx }
func (c *sessionTestRealConn) Fd() wknet.NetFd      { return wknet.NetFd{} }

type sessionTestConnManager struct {
	service.IConnManager
	conn wknet.Conn
}

func (m sessionTestConnManager) GetConn(int64) wknet.Conn   { return m.conn }
func (m sessionTestConnManager) GetConnByFd(int) wknet.Conn { return m.conn }

func TestCheckConnValidRejectsReusedConnection(t *testing.T) {
	previous := service.ConnManager
	t.Cleanup(func() { service.ConnManager = previous })
	tests := []struct {
		name string
		edit func(*eventbus.Conn)
	}{
		{name: "另一个用户", edit: func(c *eventbus.Conn) { c.Uid = "other" }},
		{name: "重新连接", edit: func(c *eventbus.Conn) { c.Uptime++ }},
		{name: "不同密钥", edit: func(c *eventbus.Conn) { c.AesKey = []byte("new-key") }},
		{name: "同秒复用但会话不同", edit: func(c *eventbus.Conn) { c.SessionId = "new-session" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "session", Auth: true, AesKey: []byte("old-key")}
			current := &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "session", Auth: true, AesKey: []byte("old-key")}
			tt.edit(current)
			service.ConnManager = sessionTestConnManager{conn: &sessionTestRealConn{ctx: current}}
			if got, err := CheckConnValidAndGetRealConn(target); err == nil || got != nil {
				t.Fatal("旧会话不应命中复用连接 ID 的新会话")
			}
		})
	}
}

func TestCheckConnValidPreservesHandshakeAndAuthenticatedSession(t *testing.T) {
	previous := service.ConnManager
	t.Cleanup(func() { service.ConnManager = previous })
	for _, auth := range []bool{false, true} {
		conn := &eventbus.Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "session", Auth: auth}
		real := &sessionTestRealConn{ctx: conn}
		service.ConnManager = sessionTestConnManager{conn: real}
		if got, err := CheckConnValidAndGetRealConn(conn); err != nil || got != real {
			t.Fatalf("相同会话应保持原握手或写入路径：auth=%t err=%v", auth, err)
		}
	}
}
