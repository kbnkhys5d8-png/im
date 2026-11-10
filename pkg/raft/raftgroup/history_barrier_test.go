package raftgroup

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

type historyBarrierRaft struct {
	IRaft
	mu      sync.Mutex
	last    atomic.Uint64
	applied atomic.Uint64
}

func (r *historyBarrierRaft) Key() string          { return "history-pending" }
func (r *historyBarrierRaft) IsLeader() bool       { return true }
func (r *historyBarrierRaft) LastLogIndex() uint64 { return r.last.Load() }
func (r *historyBarrierRaft) LastTerm() uint32     { return 1 }
func (r *historyBarrierRaft) AppliedIndex() uint64 { return r.applied.Load() }
func (r *historyBarrierRaft) KeepAlive()           {}
func (r *historyBarrierRaft) Lock()                { r.mu.Lock() }
func (r *historyBarrierRaft) Unlock()              { r.mu.Unlock() }
func (r *historyBarrierRaft) Step(e types.Event) error {
	if e.Type == types.Propose {
		r.last.Store(e.Logs[len(e.Logs)-1].Index)
	}
	return nil
}

func TestHistoryBarrierIncludesTimedOutProposalStillInQueue(t *testing.T) {
	rg := New(NewOptions())
	defer func() { rg.Stop(); rg.goPool.Release() }()
	node := &historyBarrierRaft{}
	rg.AddRaft(node)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	// 保持事件循环暂停，真实提案已进队列但取消返回，LastLogIndex 尚未更新。
	if _, err := rg.proposeBatchTimeout(canceled, node, types.ProposeReqSet{{Id: 601}}, nil); err == nil {
		t.Fatal("未复现取消返回")
	}
	if node.LastLogIndex() != 0 {
		t.Fatal("提案已提前处理")
	}
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	drained := make(chan error, 1)
	go func() { drained <- rg.WaitForHistory(ctx, node.Key()) }()
	for {
		rg.mq.mu.Lock()
		queued := len(rg.mq.nodrop)
		rg.mq.mu.Unlock()
		if queued == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("屏障未排队")
		case <-time.After(time.Millisecond):
		}
	}
	rg.handleReceivedEvents()
	select {
	case err := <-drained:
		t.Fatalf("遗漏队列中提案而提前通过: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	node.applied.Store(1)
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
}
