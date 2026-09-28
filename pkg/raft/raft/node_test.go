package raft

import (
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/assert"
)

// ==================== Test Helpers ====================

// newTestNode creates a Node with sensible defaults for testing.
// ElectionOn=false, ElectionInterval=1000 (prevent accidental elections), Advance=noop.
func newTestNode(nodeId uint64, replicas []uint64) *Node {
	opts := NewOptions(
		WithNodeId(nodeId),
		WithReplicas(replicas),
		WithElectionOn(false),
		WithElectionInterval(1000),
		WithAdvance(func() {}),
		WithKey("test"),
	)
	return NewNode(0, types.RaftState{}, opts)
}

// collectEvents calls Ready() and returns all pending events.
func collectEvents(n *Node) []types.Event {
	return n.Ready()
}

// clearEvents drains all events from the node.
func clearEvents(n *Node) {
	n.Ready()
}

// findEvent returns the first event of the given type and true, or zero-value and false.
func findEvent(events []types.Event, t types.EventType) (types.Event, bool) {
	for _, e := range events {
		if e.Type == t {
			return e, true
		}
	}
	return types.Event{}, false
}

// findEventsOfType returns all events matching the given type.
func findEventsOfType(events []types.Event, t types.EventType) []types.Event {
	var result []types.Event
	for _, e := range events {
		if e.Type == t {
			result = append(result, e)
		}
	}
	return result
}

// countEvents counts events of the given type.
func countEvents(events []types.Event, t types.EventType) int {
	return len(findEventsOfType(events, t))
}

// makeLeader transitions node to leader and clears initial events.
func makeLeader(n *Node, term uint32) {
	n.BecomeLeader(term)
	clearEvents(n)
}

// makeFollower transitions node to follower and clears initial events.
func makeFollower(n *Node, term uint32, leaderId uint64) {
	n.BecomeFollower(term, leaderId)
	clearEvents(n)
}

// makeLearner transitions node to learner and clears initial events.
func makeLearner(n *Node, term uint32, leaderId uint64) {
	n.BecomeLearner(term, leaderId)
	clearEvents(n)
}

// tickN calls Tick N times.
func tickN(n *Node, count int) {
	for i := 0; i < count; i++ {
		n.Tick()
	}
}

// proposeAndStore proposes data, collects the StoreReq, then simulates StoreResp.
// Returns the last log index after proposal.
func proposeAndStore(n *Node, data []byte) uint64 {
	evt := n.NewPropose(data)
	n.Step(evt)
	events := collectEvents(n)
	for _, e := range events {
		if e.Type == types.StoreReq {
			lastIdx := e.Logs[len(e.Logs)-1].Index
			n.Step(types.Event{
				Type:   types.StoreResp,
				Index:  lastIdx,
				Reason: types.ReasonOk,
			})
			collectEvents(n) // drain events from StoreResp
			return lastIdx
		}
	}
	return n.LastLogIndex()
}

// ==================== Node Constructor Tests ====================

func TestNewNode_SingleNode_BecomesLeader(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	assert.Equal(t, types.RoleLeader, n.cfg.Role)
	assert.Equal(t, uint64(1), n.cfg.Leader)
	assert.Equal(t, uint32(1), n.cfg.Term)
}

func TestNewNode_MultiNode_BecomesFollower(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	assert.Equal(t, types.RoleFollower, n.cfg.Role)
	assert.Equal(t, uint64(0), n.cfg.Leader)
}

func TestNewNode_EmptyReplicas_NoRole(t *testing.T) {
	n := newTestNode(1, []uint64{})
	assert.Equal(t, types.RoleUnknown, n.cfg.Role)
	assert.Nil(t, n.stepFunc)
}

func TestNewNode_WithRaftState(t *testing.T) {
	opts := NewOptions(
		WithNodeId(1),
		WithReplicas([]uint64{1, 2, 3}),
		WithElectionOn(false),
		WithElectionInterval(1000),
		WithAdvance(func() {}),
		WithKey("test"),
	)
	state := types.RaftState{
		LastLogIndex: 10,
		LastTerm:     3,
		AppliedIndex: 5,
	}
	n := NewNode(8, state, opts)
	assert.Equal(t, uint64(10), n.queue.lastLogIndex)
	assert.Equal(t, uint64(5), n.queue.appliedIndex)
	assert.Equal(t, uint32(3), n.cfg.Term)
	assert.Equal(t, uint64(8), n.lastTermStartIndex.Index)
}

func TestHasReady_NoEvents(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	clearEvents(n)
	assert.False(t, n.HasReady())
}

func TestHasReady_WithEvents(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	clearEvents(n)
	// Propose something to generate events
	evt := n.NewPropose([]byte("data"))
	n.Step(evt)
	assert.True(t, n.HasReady())
}

func TestReady_ClearsEvents(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	clearEvents(n)
	evt := n.NewPropose([]byte("data"))
	n.Step(evt)
	events := n.Ready()
	assert.True(t, len(events) > 0)
	// After Ready, events should be cleared
	events2 := n.Ready()
	assert.Equal(t, 0, len(events2))
}

func TestNewPropose_CreatesCorrectEvent(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	clearEvents(n)
	data := []byte("hello")
	evt := n.NewPropose(data)
	assert.Equal(t, types.Propose, evt.Type)
	assert.Equal(t, 1, len(evt.Logs))
	assert.Equal(t, n.cfg.Term, evt.Logs[0].Term)
	assert.Equal(t, n.queue.lastLogIndex+1, evt.Logs[0].Index)
	assert.Equal(t, data, evt.Logs[0].Data)
}

func TestTermStartIndex_Tracking(t *testing.T) {
	n := newTestNode(1, []uint64{1})
	clearEvents(n)
	n.updateLastTermStartIndex(5, 100)
	assert.Equal(t, uint32(5), n.lastTermStartIndex.Term)
	assert.Equal(t, uint64(100), n.lastTermStartIndex.Index)
}

func TestNodeConcurrentStateReads(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2})
	start := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		<-start
		for i := 0; i < 1000; i++ {
			n.LeaderId()
			n.IsLeader()
			n.Config()
			n.GetReplicaLastLogIndex(2)
			n.LastLogIndex()
			n.LastLogTerm()
			n.LastTerm()
			n.CommittedIndex()
			n.AppliedIndex()
		}
	}()
	close(start)
	// 模拟事件循环独占写入，外部协程同时查询角色、配置和副本进度。
	for i := uint32(1); i <= 1000; i++ {
		n.BecomeLeader(i)
		if err := n.Step(types.Event{Type: types.SyncReq, From: 2, Index: 1}); err != nil {
			t.Fatal(err)
		}
		n.Ready()
		n.Tick()
		n.BecomeFollower(i, 2)
	}
	readers.Wait()
	if got := n.LeaderId(); got != 2 {
		t.Fatalf("leader = %d, want 2", got)
	}
}

func TestNodeConfigReturnsIndependentCopy(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2})
	cfg := n.Config()
	cfg.Replicas[0] = 99
	if got := n.Config().Replicas[0]; got != 1 {
		t.Fatalf("修改返回配置影响了节点副本列表: got %d, want 1", got)
	}
}

func TestNodeIdleReadStateDoesNotAllocate(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*Node)
	}{
		{name: "tick", run: (*Node).Tick},
		{name: "ready", run: func(n *Node) { n.Ready() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newTestNode(1, []uint64{1})
			// 空闲节点的公开状态没有变化，不应为每次时钟或查询重复分配快照。
			if allocs := testing.AllocsPerRun(1000, func() { tc.run(n) }); allocs != 0 {
				t.Fatalf("空闲操作分配次数 = %v, want 0", allocs)
			}
		})
	}
}

func TestNodeConcurrentKeepAlive(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*Node) error
	}{
		{name: "领导者同步请求", run: func(n *Node) error {
			return n.Step(types.Event{
				Type: types.SyncReq, From: 2, To: 1, Term: 1,
				Index: 1, Reason: types.ReasonOnlySync,
			})
		}},
		{name: "空闲时钟", run: func(n *Node) error {
			n.Tick()
			return nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2})
			makeLeader(n, 1)
			start := make(chan struct{})
			var writers sync.WaitGroup
			writers.Add(1)
			go func() {
				defer writers.Done()
				<-start
				for i := 0; i < 1024; i++ {
					// 复现外部提案持锁保活，事件线程不持同一把锁的调用边界。
					n.Lock()
					n.KeepAlive()
					n.Unlock()
				}
			}()
			defer writers.Wait()
			close(start)
			// 只有当前协程消费事件和时钟，不能用并发 Step/Tick 制造无效场景。
			for i := 0; i < 1024; i++ {
				if err := tc.run(n); err != nil {
					t.Fatal(err)
				}
				n.Ready()
			}
		})
	}
}
