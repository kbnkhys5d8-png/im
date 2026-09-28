package raftgroup

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	wt "github.com/WuKongIM/WuKongIM/pkg/wait"
	"github.com/stretchr/testify/require"
)

func TestSingleProposalForwardsAfterLocalLeaderChange(t *testing.T) {
	for _, oldApplied := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("old_applied_%d", oldApplied), func(t *testing.T) {
			rg, node, transport := newLeaderChangeGroup(t)
			// 同时覆盖旧候选索引已经应用的情况，不能把旧等待结果复用给新提案。
			node.applied.Store(oldApplied)
			done := startLeaderChangeProposal(t, rg, node, false, 2)
			var sent types.Event
			select {
			case sent = <-transport.sent:
			case result := <-done:
				t.Fatalf("proposal returned before forwarding after leader change: %v", result.err)
			case <-time.After(time.Second):
				t.Fatal("proposal was not forwarded")
			}
			require.Equal(t, uint64(2), sent.To)
			require.Equal(t, uint64(1), sent.From)
			require.Equal(t, types.SendPropose, sent.Type)
			var reqs types.ProposeReqSet
			require.NoError(t, reqs.Unmarshal(sent.Logs[0].Data))
			require.Equal(t, types.ProposeReqSet{{Id: 42, Data: []byte("config")}}, reqs)
			require.Zero(t, node.LastLogIndex(), "rejected proposal must not append a local log")
			require.Equal(t, int32(1), node.proposes.Load())
			rg.fowardProposeWait.Trigger("42", types.ProposeRespSet{{Id: 42, Index: 7}})
			// 第二次读取应用进度意味着开始登记新索引；桶锁保证登记完成后再检查。
			for i := 0; i < 2; i++ {
				select {
				case <-node.readApplied:
				case <-time.After(time.Second):
					t.Fatal("forwarded apply wait was not registered")
				}
			}
			bucket := rg.wait.buckets[rg.wait.bucketIndex(node.Key())]
			bucket.mu.Lock()
			pending := len(bucket.progresses)
			var index uint64
			if pending == 1 {
				index = bucket.progresses[0].maxIndex
			}
			bucket.mu.Unlock()
			require.Equal(t, 1, pending)
			require.Equal(t, uint64(7), index)
			rg.wait.didApply(node.Key(), 1)
			select {
			case result := <-done:
				t.Fatalf("old index completed the new proposal: %+v", result)
			default:
			}
			node.applied.Store(7)
			rg.wait.didApply(node.Key(), 7)
			result := receiveLeaderChangeResult(t, done)
			require.NoError(t, result.err)
			require.Equal(t, &types.ProposeResp{Id: 42, Index: 7}, result.resp)
			require.Empty(t, transport.sent, "must forward at most once")
			require.Empty(t, bucket.progresses)
		})
	}
}

func TestLocalProposalRejectionDoesNotRetryUnsafeCases(t *testing.T) {
	otherErr := errors.New("unknown proposal outcome")
	for _, tt := range []struct {
		name   string
		leader uint64
		batch  bool
		err    error
		stop   bool
	}{
		{name: "batch_contract_unchanged", leader: 2, batch: true},
		{name: "unknown_leader", leader: 0},
		{name: "self_leader", leader: 1, err: types.ErrNotLeader},
		{name: "unknown_error", leader: 2, err: otherErr},
		{name: "wrapped_error", leader: 2, err: fmt.Errorf("outcome: %w", types.ErrNotLeader)},
		{name: "stopped_group", leader: 2, stop: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rg, node, transport := newLeaderChangeGroup(t)
			node.proposeErr = tt.err
			if tt.stop {
				rg.Stop()
			}
			done := startLeaderChangeProposal(t, rg, node, tt.batch, tt.leader)
			result := receiveLeaderChangeResult(t, done)
			require.Error(t, result.err)
			if tt.err != nil {
				require.ErrorIs(t, result.err, tt.err)
			} else if !tt.stop {
				require.ErrorIs(t, result.err, types.ErrNotLeader)
			}
			require.Empty(t, transport.sent)
			bucket := rg.wait.buckets[rg.wait.bucketIndex(node.Key())]
			require.Empty(t, bucket.progresses, "rejected proposal must release its old waiter")
		})
	}
}

func TestLeaderChangeForwardErrorIsNotRetried(t *testing.T) {
	rg, node, transport := newLeaderChangeGroup(t)
	done := startLeaderChangeProposal(t, rg, node, false, 2)
	select {
	case <-transport.sent:
	case result := <-done:
		t.Fatalf("proposal was not forwarded: %v", result.err)
	case <-time.After(time.Second):
		t.Fatal("proposal was not forwarded")
	}
	forwardErr := errors.New("remote outcome unknown")
	rg.fowardProposeWait.Trigger("42", forwardErr)
	require.ErrorIs(t, receiveLeaderChangeResult(t, done).err, forwardErr)
	require.Empty(t, transport.sent)
}

func TestLeaderChangeUsesOriginalContext(t *testing.T) {
	for _, stage := range []string{"before_forward", "forward_response", "local_apply"} {
		t.Run(stage, func(t *testing.T) {
			rg, node, transport := newLeaderChangeGroup(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if stage == "before_forward" {
				// 已确认本地拒绝后取消，避免取消与本地 Step 回应竞争掩盖转发前检查。
				node.onConfig = cancel
			}
			done := runLeaderChangeProposal(t, rg, node, 2, func() leaderChangeResult {
				_, err := rg.proposeBatchUntilAppliedTimeout(ctx, node.Key(), types.ProposeReqSet{{Id: 42}}, true)
				return leaderChangeResult{err: err}
			})
			if stage != "before_forward" {
				select {
				case <-transport.sent:
				case <-time.After(time.Second):
					t.Fatal("proposal was not forwarded")
				}
				if stage == "local_apply" {
					rg.fowardProposeWait.Trigger("42", types.ProposeRespSet{{Id: 42, Index: 7}})
					for i := 0; i < 2; i++ {
						select {
						case <-node.readApplied:
						case <-time.After(time.Second):
							t.Fatal("apply wait was not registered")
						}
					}
				}
				cancel()
			}
			require.ErrorIs(t, receiveLeaderChangeResult(t, done).err, context.Canceled)
			require.Empty(t, transport.sent, "cancellation must not cause another forward")
		})
	}
}

func TestLocalProposalTimeoutIsNotForwarded(t *testing.T) {
	rg, node, transport := newLeaderChangeGroup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan leaderChangeResult, 1)
	go func() {
		_, err := rg.proposeBatchUntilAppliedTimeout(ctx, node.Key(), types.ProposeReqSet{{Id: 42}}, true)
		done <- leaderChangeResult{err: err}
	}()
	select {
	case <-rg.advanceC:
	case <-time.After(time.Second):
		t.Fatal("local proposal was not queued")
	}
	// 故意不处理已排队的提案：此时是否接纳尚不确定，不得因超时转发重试。
	cancel()
	require.EqualError(t, receiveLeaderChangeResult(t, done).err, "propose timeout")
	require.Empty(t, transport.sent)
	require.Zero(t, node.proposes.Load())
	bucket := rg.wait.buckets[rg.wait.bucketIndex(node.Key())]
	require.Empty(t, bucket.progresses)
}

func TestForwardLeaderSnapshotBoundary(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprintf("local_rejection_%t", fallback), func(t *testing.T) {
			rg, node, transport := newLeaderChangeGroup(t)
			stepResult := make(chan error, 1)
			rg.fowardProposeWait = &leaderChangeRegisterWait{
				Wait: rg.fowardProposeWait,
				afterRegister: func() {
					// 登记响应等待时再次换主：普通路径应读取新值，拒绝回退应保留已校验目标。
					stepResult <- node.Step(types.Event{Type: types.ConfChange, Config: types.Config{
						Leader: 3, Term: 3, Role: types.RoleFollower, Replicas: []uint64{1, 2, 3},
					}})
				},
			}
			// 覆盖转发返回前已在本地应用，无需依赖第二次通知也能完成。
			node.applied.Store(7)
			var done <-chan leaderChangeResult
			if fallback {
				done = startLeaderChangeProposal(t, rg, node, false, 2)
			} else {
				require.NoError(t, node.Step(types.Event{Type: types.ConfChange, Config: types.Config{
					Leader: 2, Term: 2, Role: types.RoleFollower, Replicas: []uint64{1, 2},
				}}))
				results := make(chan leaderChangeResult, 1)
				done = results
				go func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					resps, err := rg.ProposeBatchUntilAppliedTimeout(ctx, node.Key(), types.ProposeReqSet{{Id: 42}})
					result := leaderChangeResult{err: err}
					if len(resps) == 1 {
						result.resp = resps[0]
					}
					results <- result
				}()
			}
			select {
			case sent := <-transport.sent:
				target := uint64(3)
				if fallback {
					target = 2
				}
				require.Equal(t, target, sent.To)
			case <-time.After(time.Second):
				t.Fatal("proposal was not forwarded")
			}
			require.NoError(t, <-stepResult)
			require.Equal(t, uint64(3), node.LeaderId())
			rg.fowardProposeWait.Trigger("42", types.ProposeRespSet{{Id: 42, Index: 7}})
			result := receiveLeaderChangeResult(t, done)
			require.NoError(t, result.err)
			require.Equal(t, &types.ProposeResp{Id: 42, Index: 7}, result.resp)
			require.Empty(t, transport.sent)
		})
	}
}

type leaderChangeRegisterWait struct {
	wt.Wait
	afterRegister func()
}

func (w *leaderChangeRegisterWait) Register(id string) <-chan interface{} {
	waitC := w.Wait.Register(id)
	w.afterRegister()
	return waitC
}

type leaderChangeResult struct {
	resp *types.ProposeResp
	err  error
}

type leaderChangeNode struct {
	*raft.Node
	applied     atomic.Uint64
	proposes    atomic.Int32
	readApplied chan struct{}
	proposeErr  error
	onConfig    func()
}

func (n *leaderChangeNode) Config() types.Config {
	config := n.Node.Config()
	if n.onConfig != nil {
		n.onConfig()
	}
	return config
}

func (n *leaderChangeNode) AppliedIndex() uint64 {
	n.readApplied <- struct{}{}
	return n.applied.Load()
}

func (n *leaderChangeNode) Step(event types.Event) error {
	if event.Type == types.Propose {
		n.proposes.Add(1)
		if n.proposeErr != nil {
			return n.proposeErr
		}
	}
	return n.Node.Step(event)
}

type leaderChangeTransport struct {
	sent chan types.Event
}

func (t *leaderChangeTransport) Send(_ string, event types.Event) {
	t.sent <- event
}

func newLeaderChangeGroup(t *testing.T) (*RaftGroup, *leaderChangeNode, *leaderChangeTransport) {
	t.Helper()
	transport := &leaderChangeTransport{sent: make(chan types.Event, 4)}
	rg := New(NewOptions(WithTransport(transport), WithProposeTimeout(3*time.Second)))
	node := &leaderChangeNode{
		Node: raft.NewNode(0, types.RaftState{}, raft.NewOptions(
			raft.WithKey("leader-change"), raft.WithNodeId(1), raft.WithReplicas([]uint64{1}),
		)),
		readApplied: make(chan struct{}, 4),
	}
	rg.AddRaft(node)
	t.Cleanup(func() {
		select {
		case <-rg.stopper.ShouldStop():
		default:
			rg.Stop()
		}
		rg.goPool.Release()
	})
	require.True(t, node.IsLeader())
	return rg, node, transport
}

func startLeaderChangeProposal(t *testing.T, rg *RaftGroup, node *leaderChangeNode, batch bool, leader uint64) <-chan leaderChangeResult {
	t.Helper()
	return runLeaderChangeProposal(t, rg, node, leader, func() leaderChangeResult {
		if batch {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := rg.ProposeBatchUntilAppliedTimeout(ctx, node.Key(), types.ProposeReqSet{{Id: 42, Data: []byte("config")}})
			return leaderChangeResult{err: err}
		}
		resp, err := rg.ProposeUntilApplied(node.Key(), 42, []byte("config"))
		return leaderChangeResult{resp: resp, err: err}
	})
}

func runLeaderChangeProposal(t *testing.T, rg *RaftGroup, node *leaderChangeNode, leader uint64, propose func() leaderChangeResult) <-chan leaderChangeResult {
	t.Helper()
	// 不启动事件循环：先排入配置切换，再让外部调用按旧快照排入提案。
	rg.AddEvent(node.Key(), types.Event{Type: types.ConfChange, Config: types.Config{
		Leader: leader, Term: 2, Role: types.RoleFollower, Replicas: []uint64{1, 2},
	}})
	done := make(chan leaderChangeResult, 1)
	go func() { done <- propose() }()
	select {
	case <-rg.advanceC:
	case <-time.After(time.Second):
		t.Fatal("local proposal was not queued")
	}
	require.True(t, rg.handleReceivedEvents())
	return done
}

func receiveLeaderChangeResult(t *testing.T, done <-chan leaderChangeResult) leaderChangeResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(4 * time.Second):
		t.Fatal("proposal did not finish within its original timeout")
		return leaderChangeResult{}
	}
}
