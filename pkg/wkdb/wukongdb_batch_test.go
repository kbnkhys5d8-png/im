package wkdb

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
)

type failingPhysicalBatch struct {
	failAt      int
	operations  int
	commitCalls int
	err         error
	commitErr   error
}

func (b *failingPhysicalBatch) operation() error {
	b.operations++
	if b.operations == b.failAt {
		return b.err
	}
	return nil
}

func (b *failingPhysicalBatch) Set([]byte, []byte, *pebble.WriteOptions) error {
	return b.operation()
}

func (b *failingPhysicalBatch) Delete([]byte, *pebble.WriteOptions) error {
	return b.operation()
}

func (b *failingPhysicalBatch) DeleteRange([]byte, []byte, *pebble.WriteOptions) error {
	return b.operation()
}

func (b *failingPhysicalBatch) Commit(*pebble.WriteOptions) error {
	b.commitCalls++
	return b.commitErr
}

func (b *failingPhysicalBatch) Close() error { return nil }

func TestBatchDBOperationErrorDoesNotCommitPhysicalBatch(t *testing.T) {
	injected := errors.New("injected set failure")
	physical := &failingPhysicalBatch{failAt: 2, err: injected}
	first := &Batch{
		setKvs: []kv{{key: []byte("message"), val: []byte("m")}},
		waitC:  make(chan error, 1),
	}
	second := &Batch{
		setKvs: []kv{{key: []byte("outbox"), val: []byte("o")}},
		waitC:  make(chan error, 1),
	}
	db := &BatchDB{newPhysicalBatch: func() physicalBatch { return physical }}

	db.executeBatch([]*Batch{first, second})

	if physical.commitCalls != 0 {
		t.Fatalf("Commit calls = %d, want 0", physical.commitCalls)
	}
	for index, batch := range []*Batch{first, second} {
		if err := <-batch.waitC; !errors.Is(err, injected) {
			t.Fatalf("batch %d error = %v, want %v", index, err, injected)
		}
	}
}

func TestBatchDBCommitFailureCompletesEveryLogicalBatch(t *testing.T) {
	injected := errors.New("injected commit failure")
	physical := &failingPhysicalBatch{failAt: -1, commitErr: injected}
	first := &Batch{
		setKvs: []kv{{key: []byte("message"), val: []byte("m")}},
		waitC:  make(chan error, 1),
	}
	second := &Batch{
		setKvs: []kv{{key: []byte("outbox"), val: []byte("o")}},
		waitC:  make(chan error, 1),
	}
	db := &BatchDB{newPhysicalBatch: func() physicalBatch { return physical }}

	db.executeBatch([]*Batch{first, second})

	if physical.commitCalls != 1 {
		t.Fatalf("Commit calls = %d, want 1", physical.commitCalls)
	}
	for index, batch := range []*Batch{first, second} {
		if err := <-batch.waitC; !errors.Is(err, injected) {
			t.Fatalf("batch %d error = %v, want %v", index, err, injected)
		}
	}
}

func TestBatchCommitWaitDoesNotRaceWithRelease(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	if err != nil {
		t.Fatal(err)
	}
	batchDB := NewBatchDB(0, db)
	batchDB.Start()
	t.Cleanup(func() {
		batchDB.Stop()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	const workers = 32
	const commitsPerWorker = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		go func() {
			defer wg.Done()
			for commit := 0; commit < commitsPerWorker; commit++ {
				batch := batchDB.NewBatch()
				batch.Set([]byte(fmt.Sprintf("%d/%d", worker, commit)), []byte("value"))
				if err := batch.CommitWait(); err != nil {
					t.Errorf("CommitWait: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
