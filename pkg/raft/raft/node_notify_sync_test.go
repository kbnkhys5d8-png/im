package raft

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestNotifySyncHigherTermUsesNewLeader(t *testing.T) {
	for _, role := range []types.Role{types.RoleFollower, types.RoleLearner} {
		t.Run(role.String(), func(t *testing.T) {
			n := newTestNode(1, []uint64{1, 2, 3})
			if role == types.RoleLearner {
				makeLearner(n, 3, 2)
			} else {
				makeFollower(n, 3, 2)
			}
			n.suspend = true

			// 新领导已持久化日志后发出的同步通知，必须立即指向新领导。
			require.NoError(t, n.Step(types.Event{Type: types.NotifySync, Term: 4, From: 3, To: 1}))
			require.Equal(t, role, n.cfg.Role)
			require.Equal(t, uint32(4), n.cfg.Term)
			require.Equal(t, uint64(3), n.cfg.Leader)
			require.False(t, n.suspend)
			event, ok := findEvent(collectEvents(n), types.SyncReq)
			require.True(t, ok)
			require.Equal(t, uint64(3), event.To)
			require.Equal(t, uint32(4), event.Term)
		})
	}
}

func TestNotifySyncLowerTermKeepsCurrentLeader(t *testing.T) {
	n := newTestNode(1, []uint64{1, 2, 3})
	makeFollower(n, 4, 3)

	// 旧领导的延迟通知不能把当前同步目标改回去。
	require.NoError(t, n.Step(types.Event{Type: types.NotifySync, Term: 3, From: 2, To: 1}))
	require.Equal(t, uint64(3), n.cfg.Leader)
	events := collectEvents(n)
	require.Empty(t, findEventsOfType(events, types.SyncReq))
	require.Len(t, findEventsOfType(events, types.TermResp), 1)
}
