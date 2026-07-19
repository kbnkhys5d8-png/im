package wkdb

import (
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble"
)

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
