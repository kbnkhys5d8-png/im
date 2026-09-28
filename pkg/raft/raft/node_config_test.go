package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/assert"
)

func TestSwitchConfig_LowerVersion_Rejected(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 3, 2)
	n.cfg.Version = 5
	err := n.switchConfig(types.Config{Version: 3, Replicas: []uint64{1, 2}})
	assert.Error(t, err)
}

func TestSwitchConfig_HigherVersion_Accepted(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 3, 2)
	n.cfg.Version = 1
	err := n.switchConfig(types.Config{
		Version:  2,
		Term:     3,
		Replicas: []uint64{1, 2, 3, 4},
		Role:     types.RoleFollower,
		Leader:   2,
	})
	assert.NoError(t, err)
	assert.Equal(t, 4, len(n.cfg.Replicas))
}

func TestSwitchConfig_LowerTerm_KeepsCurrentTerm(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 5, 2)
	err := n.switchConfig(types.Config{
		Version:  1,
		Term:     3, // lower than current term 5
		Replicas: []uint64{1, 2, 3},
		Role:     types.RoleFollower,
		Leader:   2,
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(5), n.cfg.Term)
}

func TestSwitchConfig_ZeroTerm_KeepsCurrentTerm(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 5, 2)
	err := n.switchConfig(types.Config{
		Version:  1,
		Term:     0,
		Replicas: []uint64{1, 2, 3},
		Role:     types.RoleFollower,
		Leader:   2,
	})
	assert.NoError(t, err)
	assert.Equal(t, uint32(5), n.cfg.Term)
}

func TestSwitchConfig_InitialConfigKeepsResolvedRole(t *testing.T) {
	tests := []struct {
		name string
		cfg  types.Config
		role types.Role
	}{
		{name: "leader", cfg: types.Config{Replicas: []uint64{1}}, role: types.RoleLeader},
		{name: "follower", cfg: types.Config{Replicas: []uint64{1, 2}}, role: types.RoleFollower},
		{name: "learner", cfg: types.Config{Replicas: []uint64{2}, Learners: []uint64{1}}, role: types.RoleLearner},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newTestNode(1, nil)
			assert.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: tt.cfg}))
			assert.Equal(t, tt.role, n.Config().Role)
			assert.Equal(t, tt.role == types.RoleLeader, n.IsLeader())
		})
	}
}

func TestSwitchConfig_MembershipKeepsElectionState(t *testing.T) {
	tests := []struct {
		name   string
		role   types.Role
		leader uint64
	}{
		{name: "leader", role: types.RoleLeader, leader: 1},
		{name: "follower", role: types.RoleFollower, leader: 2},
		{name: "learner", role: types.RoleLearner, leader: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2})
			switch tt.role {
			case types.RoleLeader:
				n.BecomeLeader(5)
			case types.RoleFollower:
				n.BecomeFollower(5, tt.leader)
			case types.RoleLearner:
				n.BecomeLearner(5, tt.leader)
			}
			cfg := types.Config{Replicas: []uint64{1, 2}, Learners: []uint64{3}, MigrateFrom: 3, MigrateTo: 3}
			if tt.role == types.RoleLearner {
				cfg.Replicas = []uint64{2, 3}
				cfg.Learners = []uint64{1}
			}

			// 成员更新没有指定角色和领导者，应保留当前选举结果。
			assert.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: cfg}))
			assert.Equal(t, tt.role, n.Config().Role)
			assert.Equal(t, tt.leader, n.LeaderId())
			assert.Equal(t, uint32(5), n.LastTerm())
			assert.Equal(t, cfg.Replicas, n.Config().Replicas)
			assert.Equal(t, cfg.Learners, n.Config().Learners)
			assert.Equal(t, tt.role == types.RoleLeader, n.IsLeader())
		})
	}
}

func TestSwitchConfig_MembershipPromotesLearner(t *testing.T) {
	n := newTestNode(3, []uint64{1, 2})
	n.BecomeLearner(5, 1)
	assert.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{Replicas: []uint64{1, 2, 3}}}))
	assert.Equal(t, types.RoleFollower, n.Config().Role)
	assert.Equal(t, uint64(1), n.LeaderId())
	assert.Equal(t, uint32(5), n.LastTerm())
}

func TestSwitchConfig_MembershipDemotesPreviousLeader(t *testing.T) {
	tests := []struct {
		name string
		cfg  types.Config
	}{
		{name: "removed from replicas", cfg: types.Config{Replicas: []uint64{2, 3}}},
		{name: "different leader specified", cfg: types.Config{Replicas: []uint64{1, 2, 3}, Leader: 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2, 3})
			n.BecomeLeader(5)
			assert.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: tt.cfg}))
			assert.Equal(t, types.RoleFollower, n.Config().Role)
			assert.Equal(t, tt.cfg.Leader, n.LeaderId())
			assert.False(t, n.IsLeader())
			assert.ErrorIs(t, n.Step(n.NewPropose([]byte("test"))), types.ErrNotLeader)
		})
	}
}

func TestSwitchConfig_ExplicitFollowerClearsLeader(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2})
	n.BecomeLeader(5)
	assert.NoError(t, n.Step(types.Event{Type: types.ConfChange, Config: types.Config{
		Replicas: []uint64{1, 2}, Role: types.RoleFollower, Term: 6,
	}}))
	assert.Equal(t, types.RoleFollower, n.Config().Role)
	assert.Zero(t, n.LeaderId())
	assert.Equal(t, uint32(6), n.LastTerm())
}

func TestRoleChangeIfNeed_BothUnknown_OnlySelf_BecomeLeader(t *testing.T) {
	n := newTestNode(1, []uint64{})
	oldCfg := types.Config{Role: types.RoleUnknown}
	newCfg := types.Config{
		Role:     types.RoleUnknown,
		Replicas: []uint64{1},
		Term:     1,
	}
	n.roleChangeIfNeed(oldCfg, newCfg)
	assert.Equal(t, types.RoleLeader, n.cfg.Role)
}

func TestRoleChangeIfNeed_BothUnknown_MultiNode_LeaderSelf_BecomeLeader(t *testing.T) {
	n := newTestNode(1, []uint64{})
	oldCfg := types.Config{Role: types.RoleUnknown}
	newCfg := types.Config{
		Role:     types.RoleUnknown,
		Replicas: []uint64{1, 2, 3},
		Leader:   1,
		Term:     1,
	}
	n.roleChangeIfNeed(oldCfg, newCfg)
	assert.Equal(t, types.RoleLeader, n.cfg.Role)
}

func TestRoleChangeIfNeed_BothUnknown_MultiNode_NotLeader_BecomeFollower(t *testing.T) {
	n := newTestNode(1, []uint64{})
	oldCfg := types.Config{Role: types.RoleUnknown}
	newCfg := types.Config{
		Role:     types.RoleUnknown,
		Replicas: []uint64{1, 2, 3},
		Leader:   2,
		Term:     1,
	}
	n.roleChangeIfNeed(oldCfg, newCfg)
	assert.Equal(t, types.RoleFollower, n.cfg.Role)
}

func TestRoleChangeIfNeed_BothUnknown_Learner(t *testing.T) {
	n := newTestNode(1, []uint64{})
	oldCfg := types.Config{Role: types.RoleUnknown}
	newCfg := types.Config{
		Role:     types.RoleUnknown,
		Replicas: []uint64{2, 3},
		Learners: []uint64{1},
		Leader:   2,
		Term:     1,
	}
	n.roleChangeIfNeed(oldCfg, newCfg)
	assert.Equal(t, types.RoleLearner, n.cfg.Role)
}

func TestRoleChangeIfNeed_RoleChanged_FollowerToLeader(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 3, 2)
	oldCfg := types.Config{Role: types.RoleFollower, Leader: 2, Term: 3}
	newCfg := types.Config{Role: types.RoleFollower, Leader: 1, Term: 3}
	n.roleChangeIfNeed(oldCfg, newCfg)
	assert.Equal(t, types.RoleLeader, n.cfg.Role)
}

func TestRoleChangeIfNeed_RoleChanged_LeaderToFollower(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeLeader(n, 3)
	oldCfg := types.Config{Role: types.RoleLeader, Leader: 1, Term: 3}
	newCfg := types.Config{Role: types.RoleFollower, Leader: 2, Term: 4}
	n.roleChangeIfNeed(oldCfg, newCfg)
	assert.Equal(t, types.RoleFollower, n.cfg.Role)
}

func TestRoleChangeIfNeed_TermChanged(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 3, 2)
	oldCfg := types.Config{Role: types.RoleFollower, Leader: 2, Term: 3}
	newCfg := types.Config{Role: types.RoleFollower, Leader: 2, Term: 5}
	n.roleChangeIfNeed(oldCfg, newCfg)
	assert.Equal(t, uint32(5), n.cfg.Term)
}
