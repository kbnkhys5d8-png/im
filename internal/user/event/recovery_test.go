package event

import (
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
)

func recoveryTestPool(t *testing.T) (*EventPool, *poller, *userHandler) {
	t.Helper()
	e := &EventPool{}
	p := newPoller(0, e)
	e.pollers = []*poller{p}
	t.Cleanup(p.handlePool.Release)
	h := newUserHandler("u", p)
	p.waitlist.push(h)
	return e, p, h
}

func recoveryTestConn(node uint64, id int64, session string) *eventbus.Conn {
	return &eventbus.Conn{
		Uid: "u", NodeId: node, ConnId: id, SessionId: session,
		DeviceId: "d", Uptime: 10, Auth: true,
		AesKey: []byte("key"), AesIV: []byte("iv"),
	}
}

func TestConnRecoveryReplacesRemoteDirectoryButPreservesLocalAndUnconfirmed(t *testing.T) {
	e, _, h := recoveryTestPool(t)
	localNode := options.G.Cluster.NodeId
	remoteNode := localNode + 1
	local := recoveryTestConn(localNode, 1, "local")
	staleRemote := recoveryTestConn(remoteNode, 2, "stale")
	internal := recoveryTestConn(remoteNode, 3, "internal")
	internal.Internal = true
	unconfirmed := recoveryTestConn(remoteNode, 4, "unconfirmed")
	unconfirmed.Auth = false
	h.conns.add(local)
	h.conns.add(staleRemote)
	h.conns.add(internal)
	h.conns.add(unconfirmed)

	apply, needed := e.BeginConnRecovery("u", 7)
	if !needed {
		t.Fatal("新版本必须拉取连接快照")
	}
	remote := recoveryTestConn(remoteNode, 5, "fresh")
	if !apply([]*eventbus.Conn{recoveryTestConn(localNode, 1, "local"), remote}) {
		t.Fatal("完整且未过期的快照应可应用")
	}
	if e.ConnById("u", localNode, 1) != local {
		t.Fatal("恢复不得替换本地真实连接指针")
	}
	if e.ConnById("u", remoteNode, 2) != nil {
		t.Fatal("快照缺席的远端旧连接应清除")
	}
	if e.ConnById("u", remoteNode, 3) != internal || e.ConnById("u", remoteNode, 4) != unconfirmed {
		t.Fatal("内部或未认证连接应保留")
	}
	got := e.ConnById("u", remoteNode, 5)
	if got == nil || got == remote || !got.SameSession(remote) || got.LastActive == 0 {
		t.Fatal("远端认证连接应独立复制并初始化活跃时间")
	}
	remote.AesKey[0] = 'X'
	if got.AesKey[0] == 'X' {
		t.Fatal("应用后不应共享快照密钥切片")
	}
	if _, needed := e.BeginConnRecovery("u", 7); needed {
		t.Fatal("相同版本不应重复恢复")
	}
}

func TestConnRecoveryRejectsQueuedCloseAndConcurrentChanges(t *testing.T) {
	t.Run("关闭先于开始", func(t *testing.T) {
		e, _, h := recoveryTestPool(t)
		old := recoveryTestConn(options.G.Cluster.NodeId+1, 1, "old")
		h.conns.add(old)
		e.AddEvent("u", &eventbus.Event{Type: eventbus.EventConnClose, Conn: old})
		apply, needed := e.BeginConnRecovery("u", 1)
		if !needed || apply([]*eventbus.Conn{old}) {
			t.Fatal("待处理关闭事件不得被快照重新覆盖")
		}
	})
	t.Run("关闭晚于开始", func(t *testing.T) {
		e, _, h := recoveryTestPool(t)
		old := recoveryTestConn(options.G.Cluster.NodeId+1, 1, "old")
		h.conns.add(old)
		apply, _ := e.BeginConnRecovery("u", 1)
		e.AddEvent("u", &eventbus.Event{Type: eventbus.EventConnClose, Conn: old})
		if apply([]*eventbus.Conn{old}) {
			t.Fatal("快照拉取期间排队关闭应拒绝旧结果")
		}
	})
	t.Run("重连换代", func(t *testing.T) {
		e, _, h := recoveryTestPool(t)
		old := recoveryTestConn(options.G.Cluster.NodeId+1, 1, "old")
		h.conns.add(old)
		apply, _ := e.BeginConnRecovery("u", 1)
		fresh := recoveryTestConn(old.NodeId, old.ConnId, "new")
		e.UpdateConn(fresh)
		if apply([]*eventbus.Conn{old}) || e.ConnById("u", fresh.NodeId, fresh.ConnId) != fresh {
			t.Fatal("旧快照不得覆盖新会话")
		}
	})
	t.Run("并发新增", func(t *testing.T) {
		e, _, _ := recoveryTestPool(t)
		apply, _ := e.BeginConnRecovery("u", 1)
		fresh := recoveryTestConn(options.G.Cluster.NodeId+1, 2, "new")
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			e.UpdateConn(fresh)
		}()
		wg.Wait()
		if apply(nil) || e.ConnById("u", fresh.NodeId, fresh.ConnId) != fresh {
			t.Fatal("并发加入连接后不得套用空快照")
		}
	})
	t.Run("目录外关闭", func(t *testing.T) {
		e, _, _ := recoveryTestPool(t)
		old := recoveryTestConn(options.G.Cluster.NodeId+1, 9, "old")
		apply, _ := e.BeginConnRecovery("u", 1)
		e.RemoveConn(old)
		if apply([]*eventbus.Conn{old}) {
			t.Fatal("目录尚无连接时收到关闭，也不得由旧快照复活")
		}
	})
}

func TestConnRecoveryRejectsReplacedHandlerAndStaleVersions(t *testing.T) {
	e, p, h := recoveryTestPool(t)
	apply, _ := e.BeginConnRecovery("u", 1)
	p.waitlist.remove("u")
	replacement := newUserHandler("u", p)
	p.waitlist.push(replacement)
	if apply(nil) {
		t.Fatal("旧 handler 的延迟快照不得覆盖新 handler")
	}
	if h == replacement {
		t.Fatal("测试夹具必须使用不同 handler")
	}
	newer, _ := e.BeginConnRecovery("u", 3)
	older, _ := e.BeginConnRecovery("u", 2)
	if !newer(nil) || older(nil) {
		t.Fatal("先应用的新版本不得被旧版本回滚")
	}
}

func TestConnRecoveryRetriesAfterQueuedCloseCompletes(t *testing.T) {
	installUserEventTestCluster(t)
	e, _, h := recoveryTestPool(t)
	h.handler = &mockUserEventHandler{}
	old := recoveryTestConn(options.G.Cluster.NodeId+1, 1, "old")
	e.AddEvent("u", &eventbus.Event{Type: eventbus.EventConnRemove, Conn: old})
	first, _ := e.BeginConnRecovery("u", 1)
	if first(nil) {
		t.Fatal("关闭处理前不得应用快照")
	}
	h.advanceEvents(h.events())
	second, needed := e.BeginConnRecovery("u", 1)
	if !needed || !second(nil) {
		t.Fatal("关闭处理完毕后应允许重新拉取并应用")
	}
}

func TestConnRecoveryEmptySnapshotAndInvalidLocalGeneration(t *testing.T) {
	e, _, h := recoveryTestPool(t)
	localNode := options.G.Cluster.NodeId
	local := recoveryTestConn(localNode, 1, "new-local")
	remote := recoveryTestConn(localNode+1, 2, "old-remote")
	h.conns.add(local)
	h.conns.add(remote)
	apply, _ := e.BeginConnRecovery("u", 1)
	if apply([]*eventbus.Conn{recoveryTestConn(localNode, 1, "old-local")}) {
		t.Fatal("旧本地会话不得覆盖真实本地连接")
	}
	if !apply(nil) || e.ConnById("u", localNode, 1) != local || e.ConnById("u", remote.NodeId, remote.ConnId) != nil {
		t.Fatal("空快照应清远端但保留本地")
	}
	apply, _ = e.BeginConnRecovery("u", 2)
	if apply([]*eventbus.Conn{recoveryTestConn(localNode+1, 3, "")}) {
		t.Fatal("空会话标识不得导入恢复目录")
	}
}

func TestConnRecoveryRejectsInvalidSnapshotAtomically(t *testing.T) {
	remoteNode := options.G.Cluster.NodeId + 1
	for _, tc := range []struct {
		name string
		bad  func() []*eventbus.Conn
	}{
		{name: "空连接", bad: func() []*eventbus.Conn { return []*eventbus.Conn{nil} }},
		{name: "错误用户", bad: func() []*eventbus.Conn {
			c := recoveryTestConn(remoteNode, 2, "s")
			c.Uid = "other"
			return []*eventbus.Conn{c}
		}},
		{name: "空节点", bad: func() []*eventbus.Conn { return []*eventbus.Conn{recoveryTestConn(0, 2, "s")} }},
		{name: "无效连接号", bad: func() []*eventbus.Conn { return []*eventbus.Conn{recoveryTestConn(remoteNode, 0, "s")} }},
		{name: "未认证", bad: func() []*eventbus.Conn {
			c := recoveryTestConn(remoteNode, 2, "s")
			c.Auth = false
			return []*eventbus.Conn{c}
		}},
		{name: "内部连接", bad: func() []*eventbus.Conn {
			c := recoveryTestConn(remoteNode, 2, "s")
			c.Internal = true
			return []*eventbus.Conn{c}
		}},
		{name: "空会话", bad: func() []*eventbus.Conn { return []*eventbus.Conn{recoveryTestConn(remoteNode, 2, "")} }},
		{name: "重复连接", bad: func() []*eventbus.Conn { c := recoveryTestConn(remoteNode, 2, "s"); return []*eventbus.Conn{c, c} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _, h := recoveryTestPool(t)
			old := recoveryTestConn(remoteNode, 1, "old")
			h.conns.add(old)
			apply, _ := e.BeginConnRecovery("u", 1)
			valid := recoveryTestConn(remoteNode, 2, "new")
			invalid := append([]*eventbus.Conn{valid}, tc.bad()...)
			if apply(invalid) || e.ConnById("u", remoteNode, 1) != old || e.ConnById("u", remoteNode, 2) != nil {
				t.Fatal("坏快照不得部分改变目录")
			}
			if !apply([]*eventbus.Conn{valid}) {
				t.Fatal("坏快照未应用时应允许本次有效结果重试")
			}
		})
	}
}

func TestInvalidateConnRecoveryDoesNotRegisterOrLoseVersionFence(t *testing.T) {
	e, _, h := recoveryTestPool(t)
	existing := recoveryTestConn(options.G.Cluster.NodeId+1, 1, "existing")
	late := recoveryTestConn(options.G.Cluster.NodeId+2, 2, "late")
	apply, _ := e.BeginConnRecovery("u", 7)
	if !apply([]*eventbus.Conn{existing}) {
		t.Fatal("必须先恢复已存在的另一台设备")
	}
	if e.InvalidateConnRecovery(existing) {
		t.Fatal("已有会话不得反复失效缓存")
	}
	if !e.InvalidateConnRecovery(late) || e.ConnById("u", late.NodeId, late.ConnId) != nil {
		t.Fatal("未知会话只能失效，不能直接加入目录")
	}
	old, needed := e.BeginConnRecovery("u", 6)
	if !needed || old([]*eventbus.Conn{late}) {
		t.Fatal("失效后也不得用更低配置版本回滚目录")
	}
	refresh, needed := e.BeginConnRecovery("u", 7)
	if !needed {
		t.Fatal("相同配置版本也必须重新验证迟到认证")
	}
	if !e.InvalidateConnRecovery(late) || refresh([]*eventbus.Conn{existing}) {
		t.Fatal("在途旧快照必须被再次到达的新会话提示拒绝")
	}
	refresh, _ = e.BeginConnRecovery("u", 7)
	if !refresh([]*eventbus.Conn{existing, late}) || h.conns.len() != 2 {
		t.Fatal("真实快照应保留既有设备并补齐迟到设备")
	}
}

type recoveryTestRealConn struct {
	wknet.Conn
	ctx *eventbus.Conn
}

func (r *recoveryTestRealConn) Context() interface{} { return r.ctx }

type recoveryTestManager struct {
	service.IConnManager
	conn    wknet.Conn
	removed int
}

func (m *recoveryTestManager) GetConn(int64) wknet.Conn { return m.conn }
func (m *recoveryTestManager) RemoveConn(wknet.Conn)    { m.removed++ }

func TestRemoveConnDoesNotRemoveOtherOwnerOrSessionSocket(t *testing.T) {
	e, _, h := recoveryTestPool(t)
	previous := service.ConnManager
	t.Cleanup(func() { service.ConnManager = previous })
	localNode := options.G.Cluster.NodeId
	remote := recoveryTestConn(localNode+1, 1, "remote")
	local := recoveryTestConn(localNode, 1, "local")
	manager := &recoveryTestManager{conn: &recoveryTestRealConn{ctx: local}}
	service.ConnManager = manager
	h.conns.add(remote)
	e.RemoveConn(remote)
	if manager.removed != 0 {
		t.Fatal("远端同 ConnId 不得删除本地 socket")
	}
	stale := recoveryTestConn(localNode, 1, "stale")
	h.conns.add(local)
	e.RemoveConn(stale)
	if manager.removed != 0 || e.ConnById("u", localNode, 1) != local {
		t.Fatal("过期本地会话不得删除新 socket 或逻辑目录")
	}
	e.RemoveConn(local)
	if manager.removed != 1 || e.ConnById("u", localNode, 1) != nil {
		t.Fatal("当前本地会话应正常移除")
	}
}
