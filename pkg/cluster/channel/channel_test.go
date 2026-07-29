package channel

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func TestChannelStepRejectsLowerTermConfig(t *testing.T) {
	for _, eventType := range []types.EventType{types.ConfChange, types.ConfigResp} {
		t.Run(eventType.String(), func(t *testing.T) {
			node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(
				raft.WithKey("stale-config"),
				raft.WithNodeId(1),
				raft.WithReplicas([]uint64{1, 2, 3}),
			))
			if err := node.Step(types.Event{
				Type: types.ConfChange,
				Config: types.Config{
					Version:  1,
					Term:     12,
					Replicas: []uint64{1, 2, 3},
					Role:     types.RoleFollower,
					Leader:   2,
				},
			}); err != nil {
				t.Fatal(err)
			}
			ch := &Channel{Node: node}

			err := ch.Step(types.Event{
				Type: eventType,
				Term: 12,
				Config: types.Config{
					Version:  2,
					Term:     11,
					Replicas: []uint64{1, 2, 3},
					Role:     types.RoleLeader,
					Leader:   1,
				},
			})

			if err == nil {
				t.Fatal("lower-term channel config unexpectedly succeeded")
			}
			cfg := node.Config()
			if cfg.Term != 12 || cfg.Leader != 2 || cfg.Role != types.RoleFollower || cfg.Version != 1 {
				t.Fatalf("raft config changed after stale update: %+v", cfg)
			}
		})
	}
}

func TestChannelStepRejectsConfigBelowResponseTerm(t *testing.T) {
	node := raft.NewNode(0, types.RaftState{}, raft.NewOptions(
		raft.WithKey("stale-config-response"),
		raft.WithNodeId(1),
		raft.WithReplicas([]uint64{1, 2, 3}),
	))
	if err := node.Step(types.Event{
		Type: types.ConfChange,
		Config: types.Config{
			Version:  1,
			Term:     11,
			Replicas: []uint64{1, 2, 3},
			Role:     types.RoleFollower,
			Leader:   2,
		},
	}); err != nil {
		t.Fatal(err)
	}
	ch := &Channel{Node: node}

	err := ch.Step(types.Event{
		Type: types.ConfigResp,
		Term: 12,
		From: 2,
		To:   1,
		Config: types.Config{
			Version:  2,
			Term:     11,
			Replicas: []uint64{1, 2, 3},
			Role:     types.RoleLeader,
			Leader:   1,
		},
	})

	if err == nil {
		t.Fatal("config below response term unexpectedly succeeded")
	}
	cfg := node.Config()
	if cfg.Term != 12 || cfg.Leader != 0 || cfg.Role != types.RoleFollower || cfg.Version != 1 {
		t.Fatalf("raft did not adopt response term safely: %+v", cfg)
	}
}
