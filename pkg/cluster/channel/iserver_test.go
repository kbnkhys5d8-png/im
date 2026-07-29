package channel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

func TestProposeBatchUntilApplied_RetriesAuthorityOnceAfterLeadershipRace(t *testing.T) {
	const (
		channelID   = "leadership-race"
		channelType = uint8(2)
	)
	cluster := &proposalTestCluster{config: wkdb.ChannelClusterConfig{
		ChannelId:   channelID,
		ChannelType: channelType,
		LeaderId:    2,
		Replicas:    []uint64{1, 2, 3},
		Term:        12,
		ConfVersion: 2,
	}}
	rpc := &proposalTestRPC{responses: types.ProposeRespSet{{
		Id:    7,
		Index: 15,
	}}}
	server := NewServer(NewOptions(
		WithNodeId(1),
		WithGroupCount(1),
		WithCluster(cluster),
		WithRPC(rpc),
		WithTransport(discardChannelTransport{}),
	))
	channelKey := wkutil.ChannelToKey(channelID, channelType)
	server.raftGroups[0].AddRaft(&leadershipRaceRaft{
		key:              channelKey,
		nodeID:           1,
		leaderCheckLimit: 1,
		stepErr:          types.ErrNotLeader,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	responses, err := server.ProposeBatchUntilAppliedTimeout(ctx, channelID, channelType, types.ProposeReqSet{{
		Id:   7,
		Data: []byte("message"),
	}})

	if err != nil {
		t.Fatal(err)
	}
	if cluster.calls != 1 {
		t.Fatalf("authority lookups = %d, want 1", cluster.calls)
	}
	if rpc.calls != 1 || rpc.nodeID != 2 {
		t.Fatalf("RPC calls/node = %d/%d, want 1/2", rpc.calls, rpc.nodeID)
	}
	if len(responses) != 1 || responses[0].Id != 7 || responses[0].Index != 15 {
		t.Fatalf("responses = %#v, want id 7 at index 15", responses)
	}
}

func TestProposeBatchUntilAppliedForLocal_RechecksAuthorityAfterLeadershipRace(t *testing.T) {
	const (
		channelID   = "local-leadership-race"
		channelType = uint8(2)
	)
	cluster := &proposalTestCluster{config: wkdb.ChannelClusterConfig{
		ChannelId:   channelID,
		ChannelType: channelType,
		LeaderId:    2,
		Replicas:    []uint64{1, 2, 3},
		Term:        12,
		ConfVersion: 2,
	}}
	server := NewServer(NewOptions(
		WithNodeId(1),
		WithGroupCount(1),
		WithCluster(cluster),
		WithTransport(discardChannelTransport{}),
	))
	channelKey := wkutil.ChannelToKey(channelID, channelType)
	server.raftGroups[0].AddRaft(&leadershipRaceRaft{
		key:              channelKey,
		nodeID:           1,
		leaderCheckLimit: 1,
		stepErr:          types.ErrNotLeader,
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := server.ProposeBatchUntilAppliedTimeoutForLocal(ctx, channelID, channelType, types.ProposeReqSet{{
		Id:   8,
		Data: []byte("message"),
	}})

	if err == nil {
		t.Fatal("local proposal unexpectedly succeeded after leadership moved to node 2")
	}
	if cluster.calls != 1 {
		t.Fatalf("authority lookups = %d, want 1", cluster.calls)
	}
}

func TestProposeBatchUntilApplied_DoesNotRetryUnknownCommitResult(t *testing.T) {
	proposalErrors := []struct {
		name string
		err  error
	}{
		{name: "timeout", err: context.DeadlineExceeded},
		{name: "unknown", err: errors.New("commit outcome unknown")},
	}
	methods := []struct {
		name    string
		propose func(*Server, context.Context, string, uint8, types.ProposeReqSet) (types.ProposeRespSet, error)
	}{
		{name: "remote-capable", propose: (*Server).ProposeBatchUntilAppliedTimeout},
		{name: "local-only", propose: (*Server).ProposeBatchUntilAppliedTimeoutForLocal},
	}

	for _, method := range methods {
		for _, proposalError := range proposalErrors {
			t.Run(method.name+"/"+proposalError.name, func(t *testing.T) {
				const (
					channelID   = "unknown-commit"
					channelType = uint8(2)
				)
				cluster := &proposalTestCluster{config: wkdb.ChannelClusterConfig{
					ChannelId:   channelID,
					ChannelType: channelType,
					LeaderId:    2,
					Replicas:    []uint64{1, 2, 3},
					Term:        12,
					ConfVersion: 2,
				}}
				rpc := &proposalTestRPC{}
				server := NewServer(NewOptions(
					WithNodeId(1),
					WithGroupCount(1),
					WithCluster(cluster),
					WithRPC(rpc),
					WithTransport(discardChannelTransport{}),
				))
				channelKey := wkutil.ChannelToKey(channelID, channelType)
				server.raftGroups[0].AddRaft(&leadershipRaceRaft{
					key:              channelKey,
					nodeID:           1,
					leaderCheckLimit: 2,
					stepErr:          proposalError.err,
				})
				if err := server.raftGroups[0].Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(server.raftGroups[0].Stop)

				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_, err := method.propose(server, ctx, channelID, channelType, types.ProposeReqSet{{
					Id:   9,
					Data: []byte("message"),
				}})

				if !errors.Is(err, proposalError.err) {
					t.Fatalf("proposal error = %v, want %v", err, proposalError.err)
				}
				if cluster.calls != 0 || rpc.calls != 0 {
					t.Fatalf("authority/RPC calls = %d/%d, want 0/0", cluster.calls, rpc.calls)
				}
			})
		}
	}
}

type proposalTestCluster struct {
	icluster.ICluster
	config wkdb.ChannelClusterConfig
	calls  int
}

func (c *proposalTestCluster) GetOrCreateChannelClusterConfigFromSlotLeader(string, uint8) (wkdb.ChannelClusterConfig, error) {
	c.calls++
	return c.config, nil
}

type proposalTestRPC struct {
	responses types.ProposeRespSet
	calls     int
	nodeID    uint64
}

func (r *proposalTestRPC) RequestChannelProposeBatchUntilApplied(nodeID uint64, _ string, _ uint8, _ types.ProposeReqSet) (types.ProposeRespSet, error) {
	r.calls++
	r.nodeID = nodeID
	return r.responses, nil
}

func (r *proposalTestRPC) RequestSlotProposeBatchUntilApplied(uint64, uint32, types.ProposeReqSet) (types.ProposeRespSet, error) {
	panic("unexpected slot proposal")
}

type leadershipRaceRaft struct {
	key              string
	nodeID           uint64
	isLeaderCalls    int
	leaderCheckLimit int
	stepErr          error
}

var _ raftgroup.IRaft = (*leadershipRaceRaft)(nil)

func (r *leadershipRaceRaft) Key() string            { return r.key }
func (r *leadershipRaceRaft) HasReady() bool         { return false }
func (r *leadershipRaceRaft) Ready() []types.Event   { return nil }
func (r *leadershipRaceRaft) Step(types.Event) error { return r.stepErr }
func (r *leadershipRaceRaft) Tick()                  {}
func (r *leadershipRaceRaft) LeaderId() uint64       { return 0 }
func (r *leadershipRaceRaft) LastLogIndex() uint64   { return 0 }
func (r *leadershipRaceRaft) LastTerm() uint32       { return 11 }
func (r *leadershipRaceRaft) CommittedIndex() uint64 { return 0 }
func (r *leadershipRaceRaft) AppliedIndex() uint64   { return 0 }
func (r *leadershipRaceRaft) NodeId() uint64         { return r.nodeID }
func (r *leadershipRaceRaft) Lock()                  {}
func (r *leadershipRaceRaft) Unlock()                {}
func (r *leadershipRaceRaft) Config() types.Config   { return types.Config{} }
func (r *leadershipRaceRaft) KeepAlive()             {}
func (r *leadershipRaceRaft) IsLeader() bool {
	r.isLeaderCalls++
	return r.isLeaderCalls <= r.leaderCheckLimit
}
