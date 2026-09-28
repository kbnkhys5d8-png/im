package raft_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func TestProposeUntilAppliedConcurrent(t *testing.T) {
	storage := &concurrentProposeStorage{testStorage: newTestStorage(1)}
	r := raft.New(newTestOptions(1, []uint64{1}, raft.WithStorage(storage)))
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	defer r.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const workers, perWorker = 8, 32
	indexes := make(chan uint64, workers*perWorker)
	errors := make(chan error, workers)
	var pending sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		pending.Add(1)
		go func(worker int) {
			defer pending.Done()
			for i := 0; i < perWorker; i++ {
				id := uint64(worker*perWorker + i + 1)
				resp, err := r.ProposeUntilAppliedTimeout(ctx, id, []byte("data"))
				if err != nil {
					errors <- err
					return
				}
				// 成功返回必须已经应用，且并发提案不能复用日志下标。
				if applied := storage.applied.Load(); applied < resp.Index {
					t.Errorf("返回时仅应用到 %d，提案下标为 %d", applied, resp.Index)
				}
				indexes <- resp.Index
			}
		}(worker)
	}
	pending.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	close(indexes)
	seen := make(map[uint64]bool)
	for index := range indexes {
		if seen[index] {
			t.Errorf("重复提案下标 %d", index)
		}
		seen[index] = true
	}
	if len(seen) != workers*perWorker {
		t.Fatalf("成功提案数 = %d, want %d", len(seen), workers*perWorker)
	}
}

func TestForwardedProposalWaitsForLocalApply(t *testing.T) {
	for _, name := range []string{"already_applied", "apply_before_response", "apply_after_response"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			storage := &concurrentProposeStorage{
				testStorage:   newTestStorage(2),
				state:         types.RaftState{LastLogIndex: 1, LastTerm: 1},
				applyStarted:  make(chan struct{}),
				applyRelease:  make(chan struct{}),
				applyFinished: make(chan struct{}),
			}
			storage.logs = []types.Log{{Id: 1, Index: 1, Term: 1, Data: []byte("data")}}
			if name == "already_applied" {
				storage.state.AppliedIndex = 1
				storage.applied.Store(1)
			}
			forwarded := make(chan struct{})
			responseSent := make(chan struct{})
			allowResponse := make(chan struct{})
			var releaseApply, releaseResponse sync.Once
			defer releaseApply.Do(func() { close(storage.applyRelease) })
			defer releaseResponse.Do(func() { close(allowResponse) })
			var r *raft.Raft
			transport := proposeTransportFunc(func(event types.Event) {
				if event.Type != types.SendPropose {
					return
				}
				close(forwarded)
				select {
				case <-allowResponse:
				case <-ctx.Done():
					return
				}
				data, _ := (types.ProposeRespSet{{Id: 1, Index: 1}}).Marshal()
				r.Step(types.Event{Type: types.SendProposeResp, Reason: types.ReasonOk, Logs: []types.Log{{Data: data}}})
				close(responseSent)
			})
			r = raft.New(newTestOptions(2, []uint64{1, 2}, raft.WithStorage(storage), raft.WithTransport(transport)))
			r.BecomeFollower(1, 1)
			if err := r.Start(); err != nil {
				t.Fatal(err)
			}
			defer r.Stop()
			done := make(chan error, 1)
			go func() {
				_, err := r.ProposeUntilAppliedTimeout(ctx, 1, []byte("data"))
				done <- err
			}()
			select {
			case <-forwarded:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if name != "apply_before_response" {
				releaseResponse.Do(func() { close(allowResponse) })
				select {
				case <-responseSent:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			if name != "already_applied" {
				if err := r.StepWait(ctx, types.Event{Type: types.Ping, From: 1, Term: 1, CommittedIndex: 1}); err != nil {
					t.Fatal(err)
				}
				select {
				case <-storage.applyStarted:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				// 领导响应不能替代本地应用；存储仍阻塞时提案必须保持等待。
				select {
				case err := <-done:
					t.Fatalf("本地尚未应用就返回: %v", err)
				default:
				}
				releaseApply.Do(func() { close(storage.applyRelease) })
				if name == "apply_before_response" {
					select {
					case <-storage.applyFinished:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
			}
			releaseResponse.Do(func() { close(allowResponse) })
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
				if storage.applied.Load() != 1 {
					t.Fatal("提案返回后日志尚未应用")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}

type proposeTransportFunc func(types.Event)

func (f proposeTransportFunc) Send(event types.Event) { f(event) }

type concurrentProposeStorage struct {
	*testStorage
	applied       atomic.Uint64
	state         types.RaftState
	applyStarted  chan struct{}
	applyRelease  chan struct{}
	applyFinished chan struct{}
}

func (s *concurrentProposeStorage) GetState() (types.RaftState, error) {
	return s.state, nil
}

func (s *concurrentProposeStorage) GetLogs(start, end, limit uint64) ([]types.Log, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneTestLogs(s.logs[start-1 : end-1]), nil
}

func (s *concurrentProposeStorage) Apply(logs []types.Log) error {
	if s.applyStarted != nil {
		close(s.applyStarted)
		<-s.applyRelease
	}
	s.applied.Store(logs[len(logs)-1].Index)
	if s.applyFinished != nil {
		close(s.applyFinished)
	}
	return nil
}
