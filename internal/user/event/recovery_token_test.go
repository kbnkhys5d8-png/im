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
