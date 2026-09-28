package handler

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type connackRecoveryStub struct {
	confirm func(context.Context, *eventbus.Conn, uint64) error
}

func (r connackRecoveryStub) Ensure(context.Context, string) error { return nil }
func (r connackRecoveryStub) ConfirmAuthenticated(ctx context.Context, conn *eventbus.Conn, leader uint64) error {
	return r.confirm(ctx, conn, leader)
}

type connackRecoverySocket struct {
	sessionHandshakeConn
	closed int
}

func (c *connackRecoverySocket) Close() error   { c.closed++; return nil }
func (c *connackRecoverySocket) IsClosed() bool { return c.closed != 0 }

func TestConnackWaitsForCurrentLeaderRecovery(t *testing.T) {
	previousOptions, previousManager, previousUser, previousRecovery := options.G, service.ConnManager, eventbus.User, service.ConnRecovery
	t.Cleanup(func() {
		options.G, service.ConnManager, eventbus.User, service.ConnRecovery = previousOptions, previousManager, previousUser, previousRecovery
	})
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "确认完成才回成功"},
		{name: "恢复失败只关闭本次连接", err: errors.New("恢复超时")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options.G = options.New()
			options.G.Cluster.NodeId = 2
			initial := &eventbus.Conn{Uid: "user", NodeId: 2, ConnId: 3, SessionId: "new", ProtoVersion: 4, AdmissionVersion: 7}
			conn := &eventbus.Conn{Uid: "user", NodeId: 2, ConnId: 3, SessionId: "new", ProtoVersion: 4, Auth: true, AdmissionVersion: 999}
			real := &connackRecoverySocket{sessionHandshakeConn: sessionHandshakeConn{ctx: initial}}
			service.ConnManager = sessionHandshakeManager{conn: real}
			u := &sessionForwardUser{current: initial}
			eventbus.RegisterUser(u)
			calls := 0
			service.ConnRecovery = connackRecoveryStub{confirm: func(ctx context.Context, got *eventbus.Conn, authLeader uint64) error {
				calls++
				if real.ctx != conn || u.current != conn || !conn.Auth || len(u.events) != 0 || authLeader != 3 {
					t.Error("必须先发布真实认证连接，确认前不得排队成功 CONNACK")
				}
				if got.AdmissionVersion != 7 {
					t.Error("接入版本必须来自 owner 原始上下文，不得使用回传版本")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("认证补登必须受截止时间限制")
				}
				return tc.err
			}}
			h := &Handler{Log: wklog.NewWKLog("connack-recovery-test")}
			h.connack(&eventbus.UserContext{Events: []*eventbus.Event{{
				Conn: conn, SourceNodeId: 3, Frame: &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonSuccess},
			}}})
			if calls != 1 {
				t.Fatalf("恢复确认调用次数=%d，期望1", calls)
			}
			if tc.err != nil {
				if real.closed != 1 || len(u.events) != 0 {
					t.Fatalf("失败不得回成功或遗留认证 socket：closed=%d writes=%d", real.closed, len(u.events))
				}
			} else if real.closed != 0 || len(u.events) != 1 {
				t.Fatalf("确认成功应保持正常回执：closed=%d writes=%d", real.closed, len(u.events))
			}
		})
	}
}

func TestConnackConfirmationNeverClosesOrAcknowledgesReplacement(t *testing.T) {
	previousOptions, previousManager, previousUser, previousRecovery := options.G, service.ConnManager, eventbus.User, service.ConnRecovery
	t.Cleanup(func() {
		options.G, service.ConnManager, eventbus.User, service.ConnRecovery = previousOptions, previousManager, previousUser, previousRecovery
	})
	for _, tc := range []struct {
		name           string
		replaceContext bool
		fail           bool
	}{
		{name: "失败期间原socket上下文换代", replaceContext: true, fail: true},
		{name: "失败期间manager改指新socket", fail: true},
		{name: "成功期间原socket上下文换代", replaceContext: true},
		{name: "成功期间manager改指新socket"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options.G = options.New()
			options.G.Cluster.NodeId = 2
			old := &eventbus.Conn{Uid: "user", NodeId: 2, ConnId: 3, SessionId: "old", ProtoVersion: 4, Auth: true}
			fresh := &eventbus.Conn{Uid: "user", NodeId: 2, ConnId: 3, SessionId: "new", ProtoVersion: 4, Auth: true}
			real := &connackRecoverySocket{sessionHandshakeConn: sessionHandshakeConn{ctx: old}}
			replacement := &connackRecoverySocket{sessionHandshakeConn: sessionHandshakeConn{ctx: fresh}}
			service.ConnManager = sessionHandshakeManager{conn: real}
			u := &sessionForwardUser{current: old}
			eventbus.RegisterUser(u)
			service.ConnRecovery = connackRecoveryStub{confirm: func(context.Context, *eventbus.Conn, uint64) error {
				if tc.replaceContext {
					real.ctx = fresh
				} else {
					service.ConnManager = sessionHandshakeManager{conn: replacement}
				}
				if tc.fail {
					return errors.New("确认失败")
				}
				return nil
			}}
			h := &Handler{Log: wklog.NewWKLog("connack-replacement-test")}
			h.connack(&eventbus.UserContext{Events: []*eventbus.Event{{
				Conn: old, SourceNodeId: 3, Frame: &wkproto.ConnackPacket{ReasonCode: wkproto.ReasonSuccess},
			}}})
			if replacement.closed != 0 || len(u.events) != 0 || (tc.replaceContext && real.closed != 0) {
				t.Fatalf("不得关闭新会话或给旧身份回执：old_closed=%d new_closed=%d writes=%d", real.closed, replacement.closed, len(u.events))
			}
			if tc.fail && !tc.replaceContext && real.closed != 1 {
				t.Fatal("manager 换代后仍应精确关闭捕获的旧 socket")
			}
		})
	}
}
