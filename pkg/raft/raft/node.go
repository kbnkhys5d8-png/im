package raft

import (
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
)

type electionState struct {
	electionElapsed           int             // 选举计时器
	randomizedElectionTimeout int             // 随机选举超时时间
	voteFor                   uint64          // 投票给了谁
	votes                     map[uint64]bool // 投票记录,key为节点id，value为是否同意
}

type syncState struct {
	replicaSync map[uint64]*SyncInfo // 同步记录
}

// type softState struct {
// 	termStartIndex *types.TermStartIndexInfo
// }

type Node struct {
	// 可变状态由事件循环独占，外部查询只读取事件处理完成后发布的快照。
	readState   atomic.Pointer[nodeReadState]
	events      []types.Event
	opts        *Options
	syncElapsed int // 同步计数
	tickFnc     func()
	stepFunc    func(event types.Event) error
	queue       *queue // 日志队列
	wklog.Log
	heartbeatElapsed int          // 心跳计时器
	cfg              types.Config // 分布式配置
	electionState                 // 选举状态
	syncState                     // 同步状态
	// 最新的任期对应的开始日志下标
	lastTermStartIndex types.TermStartIndexInfo
	onlySync           bool // 是否只同步,不做截断判断
	// softState softState // 软状态
	sync.Mutex
	truncating  bool // 截断中
	stopPropose bool // 停止提案

	suspend bool // 挂起

	// 外部提案可并发保活，空闲计数不能仅依赖事件循环独占。
	idleTick atomic.Int64

	syncing             bool // 正在同步
	syncRespTimeoutTick int  // 同步响应超时计数
}

type nodeReadState struct {
	cfg             types.Config
	lastLogIndex    uint64
	lastLogTerm     uint32
	committedIndex  uint64
	appliedIndex    uint64
	replicaLogIndex map[uint64]uint64
}

func (n *Node) publishReadState() {
	previous := n.readState.Load()
	sameConfig, sameReplicas := false, false
	if previous != nil {
		cfg := previous.cfg
		sameConfig = cfg.MigrateFrom == n.cfg.MigrateFrom && cfg.MigrateTo == n.cfg.MigrateTo &&
			cfg.Role == n.cfg.Role && cfg.Term == n.cfg.Term && cfg.Version == n.cfg.Version &&
			cfg.Leader == n.cfg.Leader && slices.Equal(cfg.Replicas, n.cfg.Replicas) &&
			slices.Equal(cfg.Learners, n.cfg.Learners)
		sameReplicas = len(previous.replicaLogIndex) == len(n.replicaSync)
		if sameReplicas {
			for id, info := range n.replicaSync {
				var index uint64
				if info != nil && info.LastSyncIndex > 0 {
					index = info.LastSyncIndex - 1
				}
				if oldIndex, ok := previous.replicaLogIndex[id]; !ok || oldIndex != index {
					sameReplicas = false
					break
				}
			}
		}
		// 时钟计数等内部状态变化不需要重新分配对外快照。
		if sameConfig && sameReplicas && previous.lastLogIndex == n.queue.lastLogIndex &&
			previous.lastLogTerm == n.lastTermStartIndex.Term &&
			previous.committedIndex == n.queue.committedIndex && previous.appliedIndex == n.queue.appliedIndex {
			return
		}
	}
	state := &nodeReadState{
		lastLogIndex:   n.queue.lastLogIndex,
		lastLogTerm:    n.lastTermStartIndex.Term,
		committedIndex: n.queue.committedIndex,
		appliedIndex:   n.queue.appliedIndex,
	}
	// 未变化的配置与副本进度保持不可变，可在新快照中安全复用。
	if sameConfig {
		state.cfg = previous.cfg
	} else {
		state.cfg = n.cfg.Clone()
	}
	if sameReplicas {
		state.replicaLogIndex = previous.replicaLogIndex
	} else {
		state.replicaLogIndex = make(map[uint64]uint64, len(n.replicaSync))
		for id, info := range n.replicaSync {
			var index uint64
			if info != nil && info.LastSyncIndex > 0 {
				index = info.LastSyncIndex - 1
			}
			state.replicaLogIndex[id] = index
		}
	}
	n.readState.Store(state)
}

func NewNode(lastTermStartLogIndex uint64, raftState types.RaftState, opts *Options) *Node {
	n := &Node{
		opts: opts,
		Log:  wklog.NewWKLog(fmt.Sprintf("raft.node[%s]", opts.Key)),
	}

	if raftState.AppliedIndex > raftState.LastLogIndex {
		n.Panic("applied index > last log index", zap.Uint64("appliedIndex", raftState.AppliedIndex), zap.Uint64("lastLogIndex", raftState.LastLogIndex))
	}

	n.cfg.Replicas = append(n.cfg.Replicas, opts.Replicas...)

	if raftState.LastTerm == 0 {
		n.cfg.Term = 1
	} else {
		n.cfg.Term = raftState.LastTerm
	}
	// 初始化日志队列
	n.queue = newQueue(opts.Key, raftState.AppliedIndex, raftState.LastLogIndex)

	// 初始化选举状态
	n.votes = make(map[uint64]bool)
	n.replicaSync = make(map[uint64]*SyncInfo)
	n.resetRandomizedElectionTimeout()

	n.lastTermStartIndex.Index = lastTermStartLogIndex
	n.lastTermStartIndex.Term = n.cfg.Term

	onlySelf := false
	if len(n.cfg.Replicas) == 1 {
		if n.cfg.Replicas[0] == opts.NodeId {
			onlySelf = true
		}
	}

	if onlySelf {
		if n.cfg.Term == 0 {
			n.cfg.Term = 1
		}
		n.BecomeLeader(n.cfg.Term)
	} else {
		if len(n.cfg.Replicas) > 0 {
			n.BecomeFollower(n.cfg.Term, None)
		}
	}

	n.publishReadState()
	return n
}

func (n *Node) Key() string {
	return n.opts.Key
}

// LastLogIndex 获取最后一条日志下标
func (n *Node) LastLogIndex() uint64 {
	return n.readState.Load().lastLogIndex
}

// LastLogTerm 获取最后一条日志任期
func (n *Node) LastLogTerm() uint32 {
	return n.readState.Load().lastLogTerm
}

// LastTerm 当前领导任期
func (n *Node) LastTerm() uint32 {
	return n.readState.Load().cfg.Term
}

// HasReady 是否有待处理的事件
func (n *Node) HasReady() bool {
	if n.queue.hasNextStoreLogs() {
		return true
	}
	if n.queue.hasNextApplyLogs() {
		return true
	}
	return len(n.events) > 0
}

// Suspend 是否挂起
func (n *Node) Suspend() bool {
	return n.suspend
}

// Ready 获取待处理的事件
func (n *Node) Ready() []types.Event {
	defer n.publishReadState()

	if n.queue.hasNextStoreLogs() {
		logs := n.queue.nextStoreLogs(0)
		if len(logs) > 0 {
			var termStartIndexInfo *types.TermStartIndexInfo
			for _, log := range logs {
				if n.lastTermStartIndex.Term != log.Term || log.Index == 1 {
					termStartIndexInfo = &types.TermStartIndexInfo{
						Term:  log.Term,
						Index: log.Index,
					}
					n.updateLastTermStartIndex(log.Term, log.Index)
					break
				}
			}
			n.sendStoreReq(logs, termStartIndexInfo)
		}
	}

	if n.queue.hasNextApplyLogs() {
		start, end := n.queue.nextApplyLogs()
		if start > 0 {
			n.sendApplyReq(start, end)
		}
	}

	events := n.events
	n.events = n.events[:0]
	return events
}

func (n *Node) LeaderId() uint64 {
	return n.readState.Load().cfg.Leader
}

func (n *Node) Config() types.Config {
	return n.readState.Load().cfg.Clone()
}

func (n *Node) IsLeader() bool {
	return n.LeaderId() == n.opts.NodeId
}

func (n *Node) isLearner(nodeId uint64) bool {
	if len(n.cfg.Learners) == 0 {
		return false
	}
	for _, learner := range n.cfg.Learners {
		if learner == nodeId {
			return true
		}
	}
	return false
}

func (n *Node) CommittedIndex() uint64 {
	return n.readState.Load().committedIndex
}

func (n *Node) AppliedIndex() uint64 {
	return n.readState.Load().appliedIndex
}

func (n *Node) NodeId() uint64 {
	return n.opts.NodeId
}

// 获取某个副本的最新日志下标（领导节点才有这个信息）
func (n *Node) GetReplicaLastLogIndex(replicaId uint64) uint64 {
	state := n.readState.Load()
	if replicaId == n.opts.NodeId {
		return state.lastLogIndex
	}
	return state.replicaLogIndex[replicaId]
}

func (n *Node) KeepAlive() {
	n.idleTick.Store(0)
}
func (n *Node) advance() {
	if n.opts.Advance != nil {
		n.opts.Advance()
	}
}

// NewPropose 提案
func (n *Node) NewPropose(data []byte) types.Event {
	return types.Event{
		Type: types.Propose,
		Logs: []types.Log{
			{
				Term:  n.cfg.Term,
				Index: n.queue.lastLogIndex + 1,
				Data:  data,
			},
		},
	}
}

func (n *Node) updateLastTermStartIndex(term uint32, index uint64) {
	n.lastTermStartIndex.Term = term
	n.lastTermStartIndex.Index = index
}
