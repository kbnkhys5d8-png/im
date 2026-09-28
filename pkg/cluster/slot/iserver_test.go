package slot

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

type readinessNode struct {
	icluster.Node
	slots []*clustertypes.Slot
}

func (n readinessNode) Slots() []*clustertypes.Slot { return n.slots }
func (n readinessNode) SlotCount() uint32           { return uint32(len(n.slots)) }

func TestWaitSlotsReadyRequiresCurrentLeaderAndTerm(t *testing.T) {
	for _, tc := range []struct {
		name   string
		leader uint64
		term   uint32
	}{
		{name: "old_leader", leader: 2, term: 2},
		{name: "old_term", leader: 3, term: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &clustertypes.Slot{Id: 0, Leader: 3, Term: 2, Replicas: []uint64{1, 2, 3}}
			s := &Server{opts: NewOptions(WithNodeId(1), WithNode(readinessNode{slots: []*clustertypes.Slot{cfg}}))}
			s.raftGroup = raftgroup.New(raftgroup.NewOptions())
			n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey("0"), raft.WithNodeId(1)))
			require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{
				Replicas: cfg.Replicas, Leader: tc.leader, Term: tc.term, Role: types.RoleFollower,
			}}))
			s.raftGroup.AddRaft(n)
			done := make(chan struct{})
			go func() {
				s.MustWaitAllSlotsReady(time.Second)
				close(done)
			}()
			// 集群配置已切换，但本地槽尚未应用；非零的旧领导不代表就绪。
			early := false
			select {
			case <-done:
				early = true
			case <-time.After(50 * time.Millisecond):
			}
			require.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{
				Replicas: cfg.Replicas, Leader: cfg.Leader, Term: cfg.Term, Role: types.RoleFollower,
			}}))
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("本地槽配置收敛后仍未就绪")
			}
			require.False(t, early, "本地槽仍为旧配置时不能报告就绪")
		})
	}
}
