package raftgroup

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

type historyBarrierState struct {
	raft  IRaft
	index uint64
	err   error
}

// 历史冻结前只排空已有提案，不追加消息或修改索引。调用方必须阻止新的业务提案。
func (rg *RaftGroup) WaitForHistory(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	barrier := make(chan historyBarrierState, 1)
	// 与 Propose 使用同一 FIFO，覆盖请求超时但尚在队列内的已接纳日志。
	rg.mq.MustAdd(Event{RaftKey: key, historyBarrier: barrier})
	rg.Advance()
	var state historyBarrierState
	select {
	case state = <-barrier:
	case <-ctx.Done():
		return ctx.Err()
	case <-rg.stopper.ShouldStop():
		return ErrGroupStopped
	}
	if state.err != nil {
		return state.err
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !state.raft.IsLeader() || rg.GetRaft(key) != state.raft {
			return types.ErrNotLeader
		}
		if state.raft.AppliedIndex() >= state.index {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-rg.stopper.ShouldStop():
			return ErrGroupStopped
		case <-ticker.C:
		}
	}
}
