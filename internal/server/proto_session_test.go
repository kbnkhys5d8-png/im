package server

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type sessionProtoConn struct {
	wknet.Conn
	ctx interface{}
}

func (c *sessionProtoConn) ID() int64                { return 1 }
func (c *sessionProtoConn) SetContext(v interface{}) { c.ctx = v }
func (c *sessionProtoConn) SetMaxIdle(time.Duration) {}

type sessionProtoUser struct {
	eventbus.IUser
	events []*eventbus.Event
}

func (u *sessionProtoUser) AddEvent(_ string, e *eventbus.Event) { u.events = append(u.events, e) }
func (u *sessionProtoUser) Advance(string)                       {}

func TestUnauthenticatedConnGeneratesUniqueInternalSession(t *testing.T) {
	previousUser, previousOptions := eventbus.User, options.G
	t.Cleanup(func() { eventbus.User, options.G = previousUser, previousOptions })
	options.G = options.New()
	u := &sessionProtoUser{}
	eventbus.RegisterUser(u)
	s := &Server{opts: options.G}
	packet := &wkproto.ConnectPacket{UID: "user", DeviceID: "device", Version: wkproto.LatestVersion}
	data, err := s.opts.Proto.EncodeFrame(packet, wkproto.LatestVersion)
	if err != nil {
		t.Fatal(err)
	}
	var previousSession string
	for i := 0; i < 2; i++ {
		conn := &sessionProtoConn{}
		ctx, consumed, err := s.handleUnauthenticatedConn(conn, data, false)
		if err != nil {
			t.Fatal(err)
		}
		if ctx == nil || ctx.SessionId == "" || ctx.SessionId == previousSession {
			t.Fatal("相同连接 ID 的两次真实连接必须使用不同的内部会话标识")
		}
		if ctx.Auth || conn.ctx != ctx || consumed != len(data) || u.events[i].Conn != ctx {
			t.Fatal("会话标识不能改变原客户端握手或绕过认证")
		}
		previousSession = ctx.SessionId
	}
}
