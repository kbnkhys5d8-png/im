package raft_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/track"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRaftFixtureStorageOwnsSnapshots(t *testing.T) {
	for _, boundary := range []string{"append", "read"} {
		t.Run(boundary, func(t *testing.T) {
			storage := newTestStorage(1)
			logs := []types.Log{{Index: 1, Term: 1, Data: []byte("original"), Record: track.Record{Path: 3}}}
			term := &types.TermStartIndexInfo{Term: 1, Index: 1}
			require.NoError(t, storage.AppendLogs(logs, term))
			if boundary == "read" {
				var err error
				logs, err = storage.GetLogs(1, 2, 0)
				require.NoError(t, err)
			}
			// 输入和读取结果均归调用者所有，修改后不能污染存储或下一次读取。
			logs[0].Data[0] = 'X'
			logs[0].Record.Path = 9
			term.Term = 2
			got, err := storage.GetLogs(1, 2, 0)
			require.NoError(t, err)
			require.Len(t, got, 1)
			assert.Equal(t, "original", string(got[0].Data))
			assert.Equal(t, uint16(3), got[0].Record.Path)
			index, err := storage.GetTermStartIndex(1)
			require.NoError(t, err)
			assert.Equal(t, uint64(1), index)
		})
	}
}

func TestRaftFixtureStorageConcurrentAccess(t *testing.T) {
	storage := newTestStorage(1)
	require.NoError(t, storage.AppendLogs([]types.Log{{Index: 1, Term: 1, Data: []byte("first")}}, nil))
	start := make(chan struct{})
	var pending sync.WaitGroup
	pending.Add(2)
	go func() {
		defer pending.Done()
		<-start
		for i := uint64(2); i <= 129; i++ {
			if err := storage.AppendLogs([]types.Log{{Index: i, Term: 1, Data: []byte("next")}}, &types.TermStartIndexInfo{Term: 1, Index: 1}); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer pending.Done()
		<-start
		for i := 0; i < 128; i++ {
			_, _ = storage.GetLogs(1, 0, 0)
			_, _ = storage.GetState()
			_, _ = storage.GetTermStartIndex(1)
			_, _ = storage.LeaderLastTerm()
			_, _ = storage.LeaderTermGreaterEqThan(1)
			_ = storage.DeleteLeaderTermStartIndexGreaterThanTerm(1)
		}
	}()
	close(start)
	pending.Wait()
	logs, err := storage.GetLogs(1, 0, 0)
	require.NoError(t, err)
	require.Len(t, logs, 129)
}

func TestRaftFixtureTransportOwnsEvent(t *testing.T) {
	storage := newTestStorage(1)
	r := raft.New(newTestOptions(1, []uint64{1}, raft.WithStorage(storage)))
	t.Cleanup(r.Stop)
	transport := &testTransport{raftMap: map[uint64]*raft.Raft{1: r}}
	event := types.Event{
		Type: types.Propose, To: 1,
		Logs: []types.Log{{Id: 1, Index: 1, Term: 1, Data: []byte("original"), Record: track.Record{Path: 3}}},
	}
	// 先排队再启动接收端，确定性验证发送后修改不会穿透到另一个节点。
	transport.Send(event)
	event.Logs[0].Data[0] = 'X'
	event.Logs[0].Record.Path = 9
	require.NoError(t, r.Start())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, r.WaitUtilCommit(ctx, 1))
	logs, err := storage.GetLogs(1, 2, 0)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	assert.Equal(t, "original", string(logs[0].Data))
	assert.Equal(t, uint16(3), logs[0].Record.Path)
}
