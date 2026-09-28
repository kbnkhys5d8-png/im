package raftgroup

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func TestRaftGroupApplyNotificationAfterReadStatePublished(t *testing.T) {
	rg := New(NewOptions(WithNotNeedApplied(true)))
	t.Cleanup(func() {
		rg.Stop()
		rg.goPool.Release()
	})
	node := &applyWaitRaft{key: "apply-published"}
	rg.AddRaft(node)
	progress := rg.wait.waitApply(node.Key(), 1)
	defer rg.wait.put(progress)

	// 暂不运行事件循环，确保工作线程已完成但 ApplyResp 仍留在队列中。
	rg.handleApplyReq(node, types.Event{Type: types.ApplyReq, StartIndex: 1, EndIndex: 2})
	rg.applyWG.Wait()
	select {
	case <-progress.waitC:
		t.Fatal("apply wait completed before the event loop published the applied index")
	default:
	}
	if got := node.AppliedIndex(); got != 0 {
		t.Fatalf("applied index changed before ApplyResp was processed: %d", got)
	}

	if !rg.handleReceivedEvents() {
		t.Fatal("ApplyResp was not queued")
	}
	select {
	case <-progress.waitC:
		if got := node.AppliedIndex(); got != 1 {
			t.Fatalf("apply wait completed with unpublished applied index: %d", got)
		}
	default:
		t.Fatal("apply wait was not notified after the applied index was published")
	}
}

func TestRaftGroupApplyStepErrorDoesNotNotify(t *testing.T) {
	rg := New(NewOptions(WithNotNeedApplied(true)))
	t.Cleanup(func() {
		rg.Stop()
		rg.goPool.Release()
	})
	node := &applyWaitRaft{key: "apply-step-error", stepErr: errors.New("step failed")}
	rg.AddRaft(node)
	progress := rg.wait.waitApply(node.Key(), 1)
	defer rg.wait.put(progress)
	rg.handleApplyReq(node, types.Event{Type: types.ApplyReq, StartIndex: 1, EndIndex: 2})
	rg.applyWG.Wait()
	rg.handleReceivedEvents()
	select {
	case <-progress.waitC:
		t.Fatal("apply wait completed after a failed ApplyResp step")
	default:
	}
}

func TestRaftGroupApplyErrorResponseDoesNotNotify(t *testing.T) {
	rg := New(NewOptions())
	t.Cleanup(func() {
		rg.Stop()
		rg.goPool.Release()
	})
	node := &applyWaitRaft{key: "apply-error-response"}
	rg.AddRaft(node)
	progress := rg.wait.waitApply(node.Key(), 1)
	defer rg.wait.put(progress)
	rg.AddEvent(node.Key(), types.Event{Type: types.ApplyResp, Reason: types.ReasonError, Index: 1})
	rg.handleReceivedEvents()
	select {
	case <-progress.waitC:
		t.Fatal("apply wait completed after a failed apply response")
	default:
	}
}

func TestApplyWaitPublishedProgress(t *testing.T) {
	for _, tt := range []struct {
		name    string
		applied uint64
		done    bool
	}{
		{name: "already_applied", applied: 10, done: true},
		{name: "newer_applied", applied: 11, done: true},
		{name: "not_applied", applied: 9, done: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			aw := newWait()
			var applied atomic.Uint64
			applied.Store(tt.applied)
			// 已处理过的通知不保留历史，登记时必须读取当前发布的进度。
			aw.didApply("published", tt.applied)
			progress := aw.waitApply("published", 10, applied.Load)
			defer aw.put(progress)
			select {
			case <-progress.waitC:
				if !tt.done {
					t.Fatal("apply wait completed before its required index")
				}
			default:
				if tt.done {
					t.Fatal("apply wait missed an already-published index")
				}
			}
			bucket := aw.buckets[aw.bucketIndex("published")]
			if tt.done {
				if len(bucket.progresses) != 0 {
					t.Fatal("completed apply wait was retained in the pending list")
				}
				return
			}
			aw.didApply("another-key", 10)
			aw.didApply("published", 9)
			select {
			case <-progress.waitC:
				t.Fatal("unrelated or earlier apply completed the wait")
			default:
			}
			applied.Store(10)
			aw.didApply("published", 10)
			select {
			case <-progress.waitC:
			default:
				t.Fatal("apply wait was not notified at the required index")
			}
		})
	}
}

func TestRaftGroupForwardedProposalDoesNotMissLocalApply(t *testing.T) {
	transport := &applyWaitTransport{}
	rg := New(NewOptions(WithTransport(transport)))
	t.Cleanup(func() {
		rg.Stop()
		rg.goPool.Release()
	})
	node := &applyWaitRaft{key: "forward-apply", leaderID: 2}
	rg.AddRaft(node)
	transport.group = rg
	notified := make(chan struct{})
	node.appliedGetter = func() uint64 {
		// 模拟读取旧快照后恰好完成应用；同一桶锁必须覆盖读取和登记。
		previous := node.applied.Load()
		node.applied.Store(1)
		bucket := rg.wait.buckets[rg.wait.bucketIndex(node.Key())]
		if bucket.mu.TryLock() {
			bucket.mu.Unlock()
			rg.wait.didApply(node.Key(), 1)
			close(notified)
		} else {
			go func() {
				rg.wait.didApply(node.Key(), 1)
				close(notified)
			}()
		}
		return previous
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resps, err := rg.ProposeBatchUntilAppliedTimeout(ctx, node.Key(), types.ProposeReqSet{{Id: 1, Data: []byte("message")}})
	select {
	case <-notified:
	case <-time.After(time.Second):
		t.Fatal("apply notifier did not finish")
	}
	if err != nil {
		t.Fatalf("forwarded proposal missed the local apply notification: %v", err)
	}
	if len(resps) != 1 || resps[0].Index != 1 || node.applied.Load() < resps[0].Index {
		t.Fatalf("unexpected applied proposal result: %+v", resps)
	}
}

func TestRaftGroupAlreadyAppliedWinsReadyCancellation(t *testing.T) {
	for _, tt := range []struct {
		name string
		stop bool
	}{
		{name: "canceled_context"},
		{name: "stopped_group", stop: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for i := 0; i < 128; i++ {
				func() {
					transport := &applyWaitTransport{}
					rg := New(NewOptions(WithTransport(transport)))
					defer rg.goPool.Release()
					defer func() {
						select {
						case <-rg.stopper.ShouldStop():
						default:
							rg.Stop()
						}
					}()
					transport.group = rg
					node := &applyWaitRaft{key: "already-applied", leaderID: 2}
					node.applied.Store(1)
					rg.AddRaft(node)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					node.appliedGetter = func() uint64 {
						// 转发回应已收到，登记时让完成和取消同时就绪，复现旧成功快路。
						if tt.stop {
							rg.stopper.Stop()
						} else {
							cancel()
						}
						return node.applied.Load()
					}
					resps, err := rg.ProposeBatchUntilAppliedTimeout(ctx, node.Key(), types.ProposeReqSet{{Id: 1}})
					if err != nil {
						t.Fatalf("iteration %d: already-applied proposal did not preserve success: %v", i, err)
					}
					if len(resps) != 1 || resps[0].Index != 1 {
						t.Fatalf("unexpected already-applied result: %+v", resps)
					}
					bucket := rg.wait.buckets[rg.wait.bucketIndex(node.Key())]
					if len(bucket.progresses) != 0 {
						t.Fatal("already-applied proposal retained a pending waiter")
					}
				}()
			}
		})
	}
}

type applyWaitRaft struct {
	IRaft
	key           string
	leaderID      uint64
	applied       atomic.Uint64
	appliedGetter func() uint64
	stepErr       error
}

func (r *applyWaitRaft) Key() string      { return r.key }
func (r *applyWaitRaft) IsLeader() bool   { return false }
func (r *applyWaitRaft) LeaderId() uint64 { return r.leaderID }
func (r *applyWaitRaft) NodeId() uint64   { return 1 }
func (r *applyWaitRaft) KeepAlive()       {}

func (r *applyWaitRaft) AppliedIndex() uint64 {
	if r.appliedGetter != nil {
		return r.appliedGetter()
	}
	return r.applied.Load()
}

func (r *applyWaitRaft) Step(event types.Event) error {
	if r.stepErr != nil {
		return r.stepErr
	}
	if event.Type == types.ApplyResp && event.Reason == types.ReasonOk {
		r.applied.Store(event.Index)
	}
	return nil
}

type applyWaitTransport struct {
	group *RaftGroup
}

func (t *applyWaitTransport) Send(_ string, event types.Event) {
	if event.Type != types.SendPropose {
		return
	}
	data, err := (types.ProposeRespSet{{Id: 1, Index: 1}}).Marshal()
	if err != nil {
		panic(err)
	}
	t.group.handleSendProposeResp(types.Event{
		Type: types.SendProposeResp, Reason: types.ReasonOk,
		Logs: []types.Log{{Data: data}},
	})
}
