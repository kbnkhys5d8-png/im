package recovery

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	userevent "github.com/WuKongIM/WuKongIM/internal/user/event"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
)

type refreshRig struct {
	leader       *Recovery
	owner        *Recovery
	cluster      *recoveryCluster
	pool         *userevent.EventPool
	ownerDir     *recoveryDirectory
	sockets      map[int64]wknet.Conn
	refreshCalls atomic.Int32
}

func newRefreshRig(t *testing.T) *refreshRig {
	t.Helper()
	previousOptions, previousUser := options.G, eventbus.User
	t.Cleanup(func() { options.G, eventbus.User = previousOptions, previousUser })
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	options.G.Poller.UserCount = 1
	rig := &refreshRig{
		cluster: newRecoveryCluster(), ownerDir: &recoveryDirectory{}, sockets: make(map[int64]wknet.Conn),
	}
	rig.pool = userevent.NewEventPool(nil)
	t.Cleanup(rig.pool.Stop)
	eventbus.RegisterUser(rig.pool)
	rig.leader = New(1, rig.cluster, eventbus.User, &recoveryConnManager{})
	rig.owner = New(2, rig.cluster, rig.ownerDir, &recoveryConnManager{get: func(id int64) wknet.Conn { return rig.sockets[id] }})
	rig.cluster.request = func(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
		var err error
		switch {
		case node == 1 && path == refreshPath:
			rig.refreshCalls.Add(1)
			if strings.Contains(string(body), "aes") || strings.Contains(string(body), "abcdef0123456789") {
				return nil, errors.New("刷新提示泄漏了密钥")
			}
			err = rig.leader.refresh(ctx, body)
		case node == 2 && path == requestPath:
			err = rig.owner.respond(ctx, body)
		case node == 1 && path == resultPath:
			err = rig.leader.receive(body)
		default:
			return nil, errors.New("unexpected refresh request")
		}
		if err != nil {
			return nil, err
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	return rig
}

func (r *refreshRig) publish(conn *eventbus.Conn) {
	r.ownerDir.local = append(r.ownerDir.local, conn)
	r.sockets[conn.ConnId] = &recoverySocket{context: conn}
}

func TestRecoveryConfirmsLateAuthenticationWithoutHeartbeat(t *testing.T) {
	for _, withExisting := range []bool{false, true} {
		name := "空快照后认证完成"
		if withExisting {
			name = "已有另一设备后认证完成"
		}
		t.Run(name, func(t *testing.T) {
			rig := newRefreshRig(t)
			if withExisting {
				existing := recoveryTestConn()
				existing.ConnId, existing.SessionId = 24, "existing-device"
				rig.publish(existing)
			}
			if err := rig.leader.Ensure(recoveryContext(t), "alice"); err != nil {
				t.Fatal(err)
			}
			late := recoveryTestConn()
			rig.publish(late)
			if err := rig.owner.ConfirmAuthenticated(recoveryContext(t), late, 3); err != nil {
				t.Fatal(err)
			}
			got := rig.pool.ConnById(late.Uid, late.NodeId, late.ConnId)
			if got == nil || !got.SameSession(late) || rig.refreshCalls.Load() != 1 {
				t.Fatal("成功确认前必须用真实快照补齐迟到认证，不依赖下一次心跳")
			}
			want := 1
			if withExisting {
				want++
			}
			if len(rig.pool.AuthedConnsByUid("alice")) != want {
				t.Fatal("补登不能丢弃已有设备")
			}
		})
	}
}

func TestRecoveryNormalAuthenticationDoesNotRequestRefresh(t *testing.T) {
	rig := newRefreshRig(t)
	conn := recoveryTestConn()
	conn.AdmissionVersion = 7
	rig.publish(conn)
	if err := rig.owner.ConfirmAuthenticated(recoveryContext(t), conn, 1); err != nil {
		t.Fatal(err)
	}
	if rig.refreshCalls.Load() != 0 {
		t.Fatal("原认证领导仍在时不得增加网络往返")
	}
}

func TestRecoveryReturningToOriginalLeaderStillConfirmsChangedVersion(t *testing.T) {
	for _, admissionVersion := range []uint64{0, 5} {
		rig := newRefreshRig(t)
		if err := rig.leader.Ensure(recoveryContext(t), "alice"); err != nil {
			t.Fatal(err)
		}
		conn := recoveryTestConn()
		conn.AdmissionVersion = admissionVersion
		rig.publish(conn)
		if err := rig.owner.ConfirmAuthenticated(recoveryContext(t), conn, 1); err != nil {
			t.Fatal(err)
		}
		if rig.refreshCalls.Load() != 1 || rig.pool.ConnById(conn.Uid, conn.NodeId, conn.ConnId) == nil {
			t.Fatal("回到原领导但版本已变或未知，也必须重新确认迟到认证")
		}
	}
}

func TestRecoveryRefreshCannotRegisterUnverifiedSession(t *testing.T) {
	rig := newRefreshRig(t)
	conn := recoveryTestConn()
	rig.publish(conn)
	if err := rig.leader.Ensure(recoveryContext(t), conn.Uid); err != nil {
		t.Fatal(err)
	}
	body := recoveryJSON(t, refreshRequest{
		UID: conn.Uid, Owner: 2, ConnID: conn.ConnId, SessionID: "forged", Leader: 1, Version: 7,
	})
	if err := rig.leader.refresh(recoveryContext(t), body); err == nil {
		t.Fatal("未被真实 socket 确认的会话不能成功补登")
	}
	got := rig.pool.ConnById(conn.Uid, 2, conn.ConnId)
	if got == nil || !got.SameSession(conn) {
		t.Fatal("伪造提示不能替换真实认证会话")
	}
}

func TestRecoveryRefreshRejectsStaleOrInvalidHints(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*refreshRequest)
	}{
		{name: "旧版本", edit: func(r *refreshRequest) { r.Version-- }},
		{name: "错误领导", edit: func(r *refreshRequest) { r.Leader = 2 }},
		{name: "未知接入节点", edit: func(r *refreshRequest) { r.Owner = 99 }},
		{name: "空会话", edit: func(r *refreshRequest) { r.SessionID = "" }},
		{name: "无效连接号", edit: func(r *refreshRequest) { r.ConnID = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newRefreshRig(t)
			req := refreshRequest{UID: "alice", Owner: 2, ConnID: 23, SessionID: "s", Leader: 1, Version: 7}
			tc.edit(&req)
			if err := rig.leader.refresh(recoveryContext(t), recoveryJSON(t, req)); err == nil {
				t.Fatal("无效刷新提示不得确认成功")
			}
			if len(rig.pool.AuthedConnsByUid("alice")) != 0 {
				t.Fatal("无效刷新提示不得登记任何连接")
			}
		})
	}
}

func TestRecoveryConfirmationRejectsClosedChangedAndCancelledSessions(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*refreshRig, context.CancelFunc)
	}{
		{name: "关闭", edit: func(r *refreshRig, _ context.CancelFunc) { r.sockets[23].(*recoverySocket).closed = true }},
		{name: "连接号复用", edit: func(r *refreshRig, _ context.CancelFunc) {
			fresh := recoveryTestConn()
			fresh.SessionId = "replacement"
			r.sockets[23] = &recoverySocket{context: fresh}
		}},
		{name: "取消", edit: func(_ *refreshRig, cancel context.CancelFunc) { cancel() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rig := newRefreshRig(t)
			conn := recoveryTestConn()
			rig.publish(conn)
			ctx, cancel := context.WithCancel(recoveryContext(t))
			defer cancel()
			original := rig.cluster.request
			rig.cluster.request = func(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
				resp, err := original(ctx, node, path, body)
				if path == refreshPath && err == nil {
					tc.edit(rig, cancel)
				}
				return resp, err
			}
			if err := rig.owner.ConfirmAuthenticated(ctx, conn, 3); err == nil {
				t.Fatal("确认期间会话失效或取消后不得返回成功")
			}
		})
	}
}

func TestRecoveryConfirmationSharesDeadlineAndRejectsMissingSession(t *testing.T) {
	rig := newRefreshRig(t)
	conn := recoveryTestConn()
	rig.publish(conn)
	ctx := recoveryContext(t)
	deadline, _ := ctx.Deadline()
	rig.cluster.request = func(requestCtx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
		got, ok := requestCtx.Deadline()
		if !ok || got.After(deadline) {
			t.Error("重试不得延长调用者截止时间")
		}
		var req refreshRequest
		if json.Unmarshal(body, &req) != nil || req.SessionID != conn.SessionId {
			t.Error("刷新只应定位当前认证会话")
		}
		return &proto.Response{Status: proto.StatusError}, nil
	}
	if err := rig.owner.ConfirmAuthenticated(ctx, conn, 3); err == nil {
		t.Fatal("当前领导未确认目标会话时不得接受成功登录")
	}
}
