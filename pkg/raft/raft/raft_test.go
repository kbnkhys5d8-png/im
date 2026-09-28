package raft_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestElection(t *testing.T) {

	raft1, raft2, raft3 := newThreeRaft()
	err := raft1.Start()
	assert.Nil(t, err)

	err = raft2.Start()
	assert.Nil(t, err)

	err = raft3.Start()
	assert.Nil(t, err)

	defer raft1.Stop()
	defer raft2.Stop()
	defer raft3.Stop()

	waitBecomeLeader(raft1, raft2, raft3)

}

func TestElection2(t *testing.T) {
	opts1, opts2, opts3 := newThreeOptions()

	s1Storage := opts1.Storage.(*testStorage)
	s2Storage := opts2.Storage.(*testStorage)
	s3Storage := opts3.Storage.(*testStorage)

	s1Storage.logs = append(s1Storage.logs, types.Log{
		Index: 1,
		Term:  1,
		Data:  []byte("log1-1"),
	})
	s2Storage.logs = append(s2Storage.logs, types.Log{
		Index: 1,
		Term:  1,
		Data:  []byte("log1-1"),
	}, types.Log{
		Index: 2,
		Term:  1,
		Data:  []byte("log1-2"),
	})

	s3Storage.logs = append(s3Storage.logs, types.Log{
		Index: 1,
		Term:  1,
		Data:  []byte("log1-1"),
	}, types.Log{
		Index: 2,
		Term:  1,
		Data:  []byte("log1-2"),
	})

	s1 := raft.New(opts1)
	s2 := raft.New(opts2)
	s3 := raft.New(opts3)

	// 设置传输层
	tt := &testTransport{
		raftMap: map[uint64]*raft.Raft{
			1: s1,
			2: s2,
			3: s3,
		},
	}

	opts1.Transport = tt
	opts2.Transport = tt
	opts3.Transport = tt

	raftStart(t, s1, s2, s3)
	defer raftStop(s1, s2, s3)

	// 无任选举多少次 s1不应该当选
	electionCount := 10
	for i := 0; i < electionCount; i++ {
		raftCampaign(t, s1, s2, s3)
		waitBecomeLeader(s1, s2, s3)
		assert.Equal(t, false, s1.IsLeader())
	}

}

func TestPropose(t *testing.T) {
	raft1, raft2, raft3 := newThreeRaft()
	err := raft1.Start()
	assert.Nil(t, err)

	err = raft2.Start()
	assert.Nil(t, err)

	err = raft3.Start()
	assert.Nil(t, err)

	defer raft1.Stop()
	defer raft2.Stop()
	defer raft3.Stop()

	waitBecomeLeader(raft1, raft2, raft3)

	leader := getLeader(raft1, raft2, raft3)
	_, err = leader.Propose(1, []byte("test"))
	assert.Nil(t, err)

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raft1.WaitUtilCommit(timeoutCtx, 1)
	raft2.WaitUtilCommit(timeoutCtx, 1)
	raft3.WaitUtilCommit(timeoutCtx, 1)

	node1Logs := raft1.Options().Storage.(*testStorage).snapshotLogs()
	node2Logs := raft2.Options().Storage.(*testStorage).snapshotLogs()
	node3Logs := raft3.Options().Storage.(*testStorage).snapshotLogs()

	assert.Equal(t, 1, len(node1Logs))
	assert.Equal(t, 1, len(node2Logs))
	assert.Equal(t, 1, len(node3Logs))

	assert.Equal(t, node1Logs[0].Index, node2Logs[0].Index, node3Logs[0].Index)
	assert.Equal(t, node1Logs[0].Data, node2Logs[0].Data, node3Logs[0].Data)

}

// 冲突一
// s1有消息M1,M2任期都为1 s2有消息M1，M3，M3的任期为2
// 测试s2成为领导后，M3应该覆盖s1的M2， 因为M3的任期更大
func TestLogConflict1(t *testing.T) {
	opts1, opts2, opts3 := newThreeOptions()

	s1Storage := opts1.Storage.(*testStorage)
	s2Storage := opts2.Storage.(*testStorage)
	s3Storage := opts3.Storage.(*testStorage)

	s1Storage.logs = append(s1Storage.logs, types.Log{
		Index: 1,
		Term:  1,
		Data:  []byte("M1"),
	}, types.Log{
		Index: 2,
		Term:  1,
		Data:  []byte("M2"),
	})
	s1Storage.saveTermStartIndex(&types.TermStartIndexInfo{
		Term:  1,
		Index: 1,
	})

	s2Storage.logs = append(s2Storage.logs, types.Log{
		Index: 1,
		Term:  1,
		Data:  []byte("M1"),
	}, types.Log{
		Index: 2,
		Term:  2,
		Data:  []byte("M3"),
	})
	s2Storage.saveTermStartIndex(&types.TermStartIndexInfo{
		Term:  1,
		Index: 1,
	})
	s2Storage.saveTermStartIndex(&types.TermStartIndexInfo{
		Term:  2,
		Index: 2,
	})

	s1 := raft.New(opts1)
	s2 := raft.New(opts2)
	s3 := raft.New(opts3)

	// 设置传输层
	tt := &testTransport{
		raftMap: map[uint64]*raft.Raft{
			1: s1,
			2: s2,
			3: s3,
		},
	}

	opts1.Transport = tt
	opts2.Transport = tt
	opts3.Transport = tt

	raftStart(t, s1, s2, s3)
	defer raftStop(s1, s2, s3)

	// s2成为领导者，观察s1的日志M3是否覆盖M2
	s2.BecomeLeader(2)

	time.Sleep(time.Millisecond * 400)

	node1Logs := s1Storage.snapshotLogs()
	node2Logs := s2Storage.snapshotLogs()
	node3Logs := s3Storage.snapshotLogs()
	for i := 0; i < 2; i++ {
		assert.Equal(t, node1Logs[i].Data, node2Logs[i].Data)
		assert.Equal(t, node1Logs[i].Term, node2Logs[i].Term)
		assert.Equal(t, node1Logs[i].Index, node2Logs[i].Index)

		assert.Equal(t, node3Logs[i].Data, node2Logs[i].Data)
		assert.Equal(t, node3Logs[i].Term, node2Logs[i].Term)
		assert.Equal(t, node3Logs[i].Index, node2Logs[i].Index)
	}

}

func TestLogConflict2(t *testing.T) {
	opts1, opts2, opts3 := newThreeOptions()

	s1Storage := opts1.Storage.(*testStorage)
	s2Storage := opts2.Storage.(*testStorage)
	s3Storage := opts3.Storage.(*testStorage)

	s1Storage.logs = append(s1Storage.logs, types.Log{
		Index: 1,
		Term:  1,
		Data:  []byte("M1"),
	}, types.Log{
		Index: 2,
		Term:  1,
		Data:  []byte("M2"),
	}, types.Log{
		Index: 3,
		Term:  1,
		Data:  []byte("M3"),
	})
	s1Storage.saveTermStartIndex(&types.TermStartIndexInfo{
		Term:  1,
		Index: 1,
	})

	s2Storage.logs = append(s2Storage.logs, types.Log{
		Index: 1,
		Term:  1,
		Data:  []byte("M1"),
	}, types.Log{
		Index: 2,
		Term:  2,
		Data:  []byte("M3"),
	})
	s2Storage.saveTermStartIndex(&types.TermStartIndexInfo{
		Term:  1,
		Index: 1,
	})
	s2Storage.saveTermStartIndex(&types.TermStartIndexInfo{
		Term:  2,
		Index: 2,
	})

	s1 := raft.New(opts1)
	s2 := raft.New(opts2)
	s3 := raft.New(opts3)

	// 设置传输层
	tt := &testTransport{
		raftMap: map[uint64]*raft.Raft{
			1: s1,
			2: s2,
			3: s3,
		},
	}

	opts1.Transport = tt
	opts2.Transport = tt
	opts3.Transport = tt

	raftStart(t, s1, s2, s3)
	defer raftStop(s1, s2, s3)

	// s2成为领导者，观察s1的日志M3是否覆盖M2
	s2.BecomeLeader(2)

	time.Sleep(time.Millisecond * 400)

	node1Logs := s1Storage.snapshotLogs()
	node2Logs := s2Storage.snapshotLogs()
	node3Logs := s3Storage.snapshotLogs()
	for i := 0; i < 2; i++ {
		assert.Equal(t, node1Logs[i].Data, node2Logs[i].Data)
		assert.Equal(t, node1Logs[i].Term, node2Logs[i].Term)
		assert.Equal(t, node1Logs[i].Index, node2Logs[i].Index)

		assert.Equal(t, node3Logs[i].Data, node2Logs[i].Data)
		assert.Equal(t, node3Logs[i].Term, node2Logs[i].Term)
		assert.Equal(t, node3Logs[i].Index, node2Logs[i].Index)
	}

}

func TestProposeUntilApplied(t *testing.T) {
	raft1, raft2, raft3 := newThreeRaft(raft.WithElectionOn(false))
	err := raft1.Start()
	require.NoError(t, err)

	err = raft2.Start()
	require.NoError(t, err)

	err = raft3.Start()
	require.NoError(t, err)

	defer raft1.Stop()
	defer raft2.Stop()
	defer raft3.Stop()

	raft1.BecomeLeader(1)
	raft2.BecomeFollower(1, 1)
	raft3.BecomeFollower(1, 1)

	_, err = raft1.ProposeUntilApplied(1, []byte("test"))
	require.NoError(t, err)

	// Wait for all nodes to commit and replicate
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, raft1.WaitUtilCommit(timeoutCtx, 1))
	require.NoError(t, raft2.WaitUtilCommit(timeoutCtx, 1))
	require.NoError(t, raft3.WaitUtilCommit(timeoutCtx, 1))

	node1Logs := raft1.Options().Storage.(*testStorage).snapshotLogs()
	node2Logs := raft2.Options().Storage.(*testStorage).snapshotLogs()
	node3Logs := raft3.Options().Storage.(*testStorage).snapshotLogs()

	require.Len(t, node1Logs, 1)
	require.Len(t, node2Logs, 1)
	require.Len(t, node3Logs, 1)

	assert.Equal(t, node1Logs[0].Index, node2Logs[0].Index, node3Logs[0].Index)
	assert.Equal(t, node1Logs[0].Data, node2Logs[0].Data, node3Logs[0].Data)
}

func TestPausedRaftControlOperations(t *testing.T) {
	r := raft.New(newTestOptions(1, []uint64{1}))
	// 先设置暂停，保证循环首次运行就处于暂停分支。
	r.Pause()
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.Resume()
		r.Stop()
	}()
	done := make(chan struct{})
	go func() {
		r.BecomeFollower(2, 2)
		r.KeepAlive()
		r.BecomeLeader(3)
		close(done)
	}()
	select {
	case <-done:
		if !r.IsLeader() {
			t.Fatal("暂停中的角色切换没有完成")
		}
	case <-time.After(time.Second):
		t.Fatal("暂停阻塞了同步角色操作")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := r.ProposeBatchTimeout(ctx, types.ProposeReqSet{{Id: 1, Data: []byte("paused")}}); err != types.ErrPaused {
		t.Fatalf("暂停中的提案错误 = %v, want %v", err, types.ErrPaused)
	}
	r.Resume()
	if _, err := r.ProposeUntilAppliedTimeout(ctx, 2, []byte("resumed")); err != nil {
		t.Fatalf("恢复后提案失败: %v", err)
	}
}

func TestPausedRaftCanStop(t *testing.T) {
	r := raft.New(newTestOptions(1, []uint64{1}))
	r.Pause()
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		r.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		// 清理旧实现的条件变量等待，避免回归失败时遗留协程。
		r.Resume()
		<-done
		t.Fatal("暂停阻塞了停止操作")
	}
}

func newTestOptions(nodeId uint64, replicas []uint64, opt ...raft.Option) *raft.Options {
	optList := make([]raft.Option, 0)
	optList = append(optList, raft.WithElectionInterval(5), raft.WithNodeId(nodeId), raft.WithReplicas(replicas), raft.WithTransport(&testTransport{}), raft.WithStorage(newTestStorage(nodeId)))
	optList = append(optList, opt...)
	opts := raft.NewOptions(optList...)
	return opts
}

type testTransport struct {
	raftMap map[uint64]*raft.Raft
}

func (t *testTransport) Send(event types.Event) {
	r, ok := t.raftMap[event.To]
	if !ok {
		return
	}
	// 直连夹具也要隔离节点内存，不能把发送方的可变轨迹和数据交给接收方共用。
	event.Logs = cloneTestLogs(event.Logs)
	event.Config = event.Config.Clone()
	if event.TermStartIndexInfo != nil {
		info := *event.TermStartIndexInfo
		event.TermStartIndexInfo = &info
	}
	r.Step(event)
}

type testStorage struct {
	mu              sync.RWMutex
	nodeId          uint64
	logs            []types.Log
	termStartIndexs []*types.TermStartIndexInfo
}

func newTestStorage(nodeId uint64) *testStorage {
	return &testStorage{
		nodeId: nodeId,
	}
}

func (s *testStorage) AppendLogs(logs []types.Log, termStartIndex *types.TermStartIndexInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 存储拥有独立副本，调用方后续修改日志或任期信息不会污染夹具。
	s.logs = append(s.logs, cloneTestLogs(logs)...)
	if termStartIndex != nil {
		info := *termStartIndex
		s.termStartIndexs = append(s.termStartIndexs, &info)
	}
	return nil
}

func (s *testStorage) saveTermStartIndex(termStartIndex *types.TermStartIndexInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	info := *termStartIndex
	s.termStartIndexs = append(s.termStartIndexs, &info)
}

func (s *testStorage) GetLogs(start, end uint64, limitSize uint64) ([]types.Log, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneTestLogs(s.logs[start-1:]), nil
}

func (s *testStorage) snapshotLogs() []types.Log {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneTestLogs(s.logs)
}

// 同时复制日志值（含轨迹）和数据切片，锁外读取不会再持有存储别名。
func cloneTestLogs(logs []types.Log) []types.Log {
	cloned := slices.Clone(logs)
	for i := range cloned {
		cloned[i].Data = slices.Clone(cloned[i].Data)
	}
	return cloned
}

func (s *testStorage) GetState() (types.RaftState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.logs) == 0 {
		return types.RaftState{}, nil
	}
	lastLog := s.logs[len(s.logs)-1]
	return types.RaftState{
		LastLogIndex: lastLog.Index,
		LastTerm:     lastLog.Term,
		AppliedIndex: lastLog.Index,
	}, nil
}

func (s *testStorage) GetTermStartIndex(term uint32) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, tsi := range s.termStartIndexs {
		if tsi.Term == term {
			return tsi.Index, nil
		}
	}
	return 0, nil
}

func (s *testStorage) TruncateLogTo(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if int(index) > len(s.logs) {
		return nil
	}
	s.logs = s.logs[:index]
	return nil
}

func (s *testStorage) DeleteLeaderTermStartIndexGreaterThanTerm(term uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	newTermStartIndexs := make([]*types.TermStartIndexInfo, 0)
	for _, tsi := range s.termStartIndexs {
		if tsi.Term <= term {
			newTermStartIndexs = append(newTermStartIndexs, tsi)
		}
	}
	s.termStartIndexs = newTermStartIndexs
	return nil
}

func (s *testStorage) LeaderTermGreaterEqThan(term uint32) (uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, tsi := range s.termStartIndexs {
		if tsi.Term >= term {
			return tsi.Term, nil
		}
	}
	return 0, nil
}

func (s *testStorage) LeaderLastTerm() (uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.termStartIndexs) == 0 {
		return 0, nil
	}
	return s.termStartIndexs[len(s.termStartIndexs)-1].Term, nil
}

func (s *testStorage) Apply(logs []types.Log) error {

	return nil
}

func (s *testStorage) SaveConfig(cfg types.Config) error {
	return nil
}

// 等到某个节点成为领导者
func waitBecomeLeader(rr ...*raft.Raft) {
	for {
		for _, r := range rr {
			if r.IsLeader() {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func getLeader(rr ...*raft.Raft) *raft.Raft {
	for _, r := range rr {
		if r.IsLeader() {
			return r
		}
	}
	return nil
}

func newThreeRaft(opt ...raft.Option) (*raft.Raft, *raft.Raft, *raft.Raft) {
	defaultOpts := append([]raft.Option{raft.WithElectionOn(true)}, opt...)

	// node1
	opts1 := newTestOptions(1, []uint64{1, 2, 3}, defaultOpts...)
	raft1 := raft.New(opts1)

	// node2
	opts2 := newTestOptions(2, []uint64{1, 2, 3}, defaultOpts...)
	raft2 := raft.New(opts2)

	// node3
	opts3 := newTestOptions(3, []uint64{1, 2, 3}, defaultOpts...)
	raft3 := raft.New(opts3)

	tt := &testTransport{
		raftMap: map[uint64]*raft.Raft{
			1: raft1,
			2: raft2,
			3: raft3,
		},
	}

	opts1.Transport = tt
	opts2.Transport = tt
	opts3.Transport = tt

	return raft1, raft2, raft3
}

func newThreeOptions(opt ...raft.Option) (*raft.Options, *raft.Options, *raft.Options) {

	defaultOpts := make([]raft.Option, 0)
	defaultOpts = append(defaultOpts, raft.WithElectionOn(true))
	defaultOpts = append(defaultOpts, opt...)

	// node1
	opts1 := newTestOptions(1, []uint64{1, 2, 3}, defaultOpts...)

	// node2
	opts2 := newTestOptions(2, []uint64{1, 2, 3}, defaultOpts...)

	// node3
	opts3 := newTestOptions(3, []uint64{1, 2, 3}, defaultOpts...)

	return opts1, opts2, opts3
}

func raftStart(t *testing.T, rafts ...*raft.Raft) {
	for _, r := range rafts {
		err := r.Start()
		assert.Nil(t, err)
	}
}

func raftCampaign(_ *testing.T, rafts ...*raft.Raft) {
	for _, r := range rafts {
		r.Step(types.Event{
			Type: types.Campaign,
		})
	}
}

func raftStop(rafts ...*raft.Raft) {
	for _, r := range rafts {
		r.Stop()
	}
}
