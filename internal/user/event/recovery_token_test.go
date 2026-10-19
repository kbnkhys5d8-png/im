package event

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
)

func TestConnRecoveryRetainsTokenFingerprint(t *testing.T) {
	pool, _, _ := recoveryTestPool(t)
	conn := recoveryTestConn(options.G.Cluster.NodeId+1, 9, "token-session")
	conn.AuthTokenHash = eventbus.TokenFingerprint("current-token")
	apply, needed := pool.BeginConnRecovery(conn.Uid, 7)
	if !needed || !apply([]*eventbus.Conn{conn}) {
		t.Fatal("合法源快照应可恢复")
	}
	got := pool.ConnById(conn.Uid, conn.NodeId, conn.ConnId)
	if got == nil || got == conn || got.AuthTokenHash != conn.AuthTokenHash || !got.SameSession(conn) {
		t.Fatal("main 的独立恢复快照必须保留撤销所需指纹及严格会话身份")
	}
}

func TestQueuedTokenTaskInvalidatesRecoverySnapshot(t *testing.T) {
	pool, _, _ := recoveryTestPool(t)
	conn := recoveryTestConn(options.G.Cluster.NodeId+1, 9, "token-session")
	apply, needed := pool.BeginConnRecovery(conn.Uid, 7)
	if !needed {
		t.Fatal("应先拉取快照")
	}
	pool.AddEvent(conn.Uid, &eventbus.Event{Type: eventbus.EventUserLeaderTask})
	if apply([]*eventbus.Conn{conn}) {
		t.Fatal("排队的 Token 撤销必须阻止更早的恢复快照回填")
	}
}

func TestConnRecoveryRetainsSourceRevocation(t *testing.T) {
	pool, _, _ := recoveryTestPool(t)
	conn := recoveryTestConn(options.G.Cluster.NodeId+1, 9, "revoked-session")
	conn.Revoked.Store(true)
	apply, _ := pool.BeginConnRecovery(conn.Uid, 7)
	if !apply([]*eventbus.Conn{conn}) {
		t.Fatal("已撤销源会话应能安全过滤")
	}
	if got := pool.ConnById(conn.Uid, conn.NodeId, conn.ConnId); got != nil && !got.Revoked.Load() {
		t.Fatal("恢复克隆丢失了源会话的撤销标记")
	}
}

func TestConnRecoveryKeepsSameGenerationRevocationReference(t *testing.T) {
	pool, _, h := recoveryTestPool(t)
	conn := recoveryTestConn(options.G.Cluster.NodeId+1, 9, "same-session")
	h.conns.add(conn)
	source := recoveryTestConn(conn.NodeId, conn.ConnId, conn.SessionId)
	apply, _ := pool.BeginConnRecovery(conn.Uid, 7)
	if !apply([]*eventbus.Conn{source}) {
		t.Fatal("同代源会话应能刷新")
	}
	// 撤销方可能已经持有目录旧指针，恢复不能令后续原子撤销写入失去作用。
	conn.Revoked.Store(true)
	if got := pool.ConnById(conn.Uid, conn.NodeId, conn.ConnId); got == nil || !got.Revoked.Load() {
		t.Fatal("同代替换使已经持有的撤销引用失效")
	}
}
