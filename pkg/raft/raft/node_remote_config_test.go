package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestConfigResponseKeepsRecipientRole(t *testing.T) {
	for _, role := range []types.Role{types.RoleFollower, types.RoleLearner} {
		t.Run(role.String(), func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2, 3})
			cfg := types.Config{Replicas: []uint64{1, 2, 3}, Leader: 2, Role: types.RoleLeader, Term: 4, Version: 2}
			if role == types.RoleLearner {
				makeLearner(n, 4, 2)
				cfg.Replicas = []uint64{2, 3}
				cfg.Learners = []uint64{1}
			} else {
				makeFollower(n, 4, 2)
			}

			// 响应携带发送节点的领导角色，接收节点不能照搬成自己的角色。
			require.NoError(t, n.Step(types.Event{Type: types.ConfigResp, From: 2, To: 1, Term: 4, Config: cfg}))
			require.Equal(t, role, n.cfg.Role)
			require.Equal(t, uint64(2), n.cfg.Leader)
			require.False(t, n.IsLeader())
			require.NoError(t, n.Step(types.Event{Type: types.NotifySync, From: 2, To: 1, Term: 4}))
			syncReq, ok := findEvent(collectEvents(n), types.SyncReq)
			require.True(t, ok)
			require.Equal(t, uint64(2), syncReq.To)
		})
	}
}
