package channel

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

func TestSearchAppliedObserverCoalescesOutOfOrderWatermarksMonotonically(t *testing.T) {
	db := openAppliedTestDB(t)
	observer := newSearchAppliedObserver(db)
	key := wkutil.ChannelToKey("out-of-order", 2)
	if err := observer.Observe(key, 2); err != nil {
		t.Fatal(err)
	}
	if err := observer.Observe(key, 1); err != nil {
		t.Fatal(err)
	}
	observer.Start()
	t.Cleanup(observer.Stop)
	waitForAppliedIndex(t, db, "out-of-order", 2)
}

func TestSearchAppliedObserverRetriesTransientFailureWithoutNewMessage(t *testing.T) {
	base := openAppliedTestDB(t)
	db := &transientAppliedDB{DB: base}
	db.failNext.Store(true)
	observer := newSearchAppliedObserver(db)
	observer.Start()
	t.Cleanup(observer.Stop)
	if err := observer.Observe(wkutil.ChannelToKey("transient", 2), 1); err != nil {
		t.Fatal(err)
	}
	waitForAppliedIndex(t, base, "transient", 1)
	deadline := time.Now().Add(time.Second)
	for observer.Ready() != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := observer.Ready(); err != nil {
		t.Fatalf("observer stayed unhealthy after retry success: %v", err)
	}
	if db.calls.Load() < 2 {
		t.Fatalf("watermark attempts = %d, want transient failure plus retry", db.calls.Load())
	}
}

func TestSearchAppliedObserverRetainsLastNewChannelWhenPrimaryQueueIsFull(t *testing.T) {
	db := openAppliedTestDB(t)
	observer := newSearchAppliedObserver(db)
	hotKey := wkutil.ChannelToKey("hot", 2)
	for i := 1; i <= searchAppliedObservationQueueSize; i++ {
		if err := observer.Observe(hotKey, uint64(i)); err != nil {
			t.Fatalf("prefill observation %d: %v", i, err)
		}
	}
	lastKey := wkutil.ChannelToKey("last-new-channel", 2)
	if err := observer.Observe(lastKey, 7); err != nil {
		t.Fatalf("overflow observation was not retained in retry state: %v", err)
	}
	if observer.Ready() == nil {
		t.Fatal("deferred retry state did not fail search readiness closed")
	}
	observer.Start()
	t.Cleanup(observer.Stop)
	waitForAppliedIndex(t, db, "last-new-channel", 7)
}

func TestSearchAppliedObserverCapacityOverflowFailsClosedWithoutOnlineRescan(t *testing.T) {
	base := openAppliedTestDB(t)
	db := &rescanDetectingDB{DB: base}
	observer := newSearchAppliedObserver(db)
	hotKey := wkutil.ChannelToKey("queue-hot", 2)
	for i := 1; i <= searchAppliedObservationQueueSize; i++ {
		if err := observer.Observe(hotKey, uint64(i)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < searchAppliedRetryCapacity; i++ {
		if err := observer.Observe(wkutil.ChannelToKey(fmt.Sprintf("retry-%d", i), 2), 1); err != nil {
			t.Fatalf("retry fill %d: %v", i, err)
		}
	}
	if err := observer.Observe(wkutil.ChannelToKey("overflow", 2), 1); !errors.Is(err, errSearchAppliedRetryCapacity) {
		t.Fatalf("capacity overflow error = %v, want retry capacity", err)
	}
	if observer.Ready() == nil {
		t.Fatal("capacity overflow did not fail search readiness closed")
	}
	if db.configReads.Load() != 0 || db.physicalReads.Load() != 0 {
		t.Fatalf("runtime overflow performed unsafe rescan: configs=%d physical=%d", db.configReads.Load(), db.physicalReads.Load())
	}
}

func TestSearchAppliedObserverFlushesBoundedBatchUnderHotProducer(t *testing.T) {
	base := openAppliedTestDB(t)
	db := &notifyingAppliedDB{DB: base, wrote: make(chan struct{}, 1)}
	observer := newSearchAppliedObserver(db)
	observer.Start()
	t.Cleanup(observer.Stop)
	stop := make(chan struct{})
	var producer sync.WaitGroup
	producer.Add(1)
	go func() {
		defer producer.Done()
		key := wkutil.ChannelToKey("always-hot", 2)
		for index := uint64(1); ; index++ {
			select {
			case <-stop:
				return
			default:
				_ = observer.Observe(key, index)
			}
		}
	}()
	select {
	case <-db.wrote:
	case <-time.After(time.Second):
		close(stop)
		producer.Wait()
		t.Fatal("hot channel starved bounded observer flush")
	}
	close(stop)
	producer.Wait()
}

func TestSearchAppliedObserverStopRaceRejectsOnlyPostBoundaryObservations(t *testing.T) {
	db := openAppliedTestDB(t)
	observer := newSearchAppliedObserver(db)
	observer.Start()
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			key := wkutil.ChannelToKey(fmt.Sprintf("race-%d", worker), 2)
			for index := uint64(1); ; index++ {
				err := observer.Observe(key, index)
				if errors.Is(err, errSearchAppliedObserverClosed) {
					return
				}
			}
		}(worker)
	}
	time.Sleep(10 * time.Millisecond)
	observer.Stop()
	workers.Wait()
	if err := observer.Observe(wkutil.ChannelToKey("after-stop", 2), 1); !errors.Is(err, errSearchAppliedObserverClosed) {
		t.Fatalf("post-stop observation error = %v, want closed", err)
	}
}

func TestSearchAppliedObserverStopDrainsAcceptedObservation(t *testing.T) {
	db := openAppliedTestDB(t)
	observer := newSearchAppliedObserver(db)
	if err := observer.Observe(wkutil.ChannelToKey("drain", 2), 4); err != nil {
		t.Fatal(err)
	}
	observer.Stop()
	applied, err := db.GetChannelAppliedIndex("drain", 2)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 4 {
		t.Fatalf("shutdown drain applied=%d, want 4", applied)
	}
}

func waitForAppliedIndex(t *testing.T, db wkdb.DB, channelID string, want uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		applied, err := db.GetChannelAppliedIndex(channelID, 2)
		if err != nil {
			t.Fatal(err)
		}
		if applied == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("applied index for %s = %d, want %d", channelID, applied, want)
		}
		time.Sleep(time.Millisecond)
	}
}

type transientAppliedDB struct {
	wkdb.DB
	failNext atomic.Bool
	calls    atomic.Int32
}

func (d *transientAppliedDB) UpdateChannelAppliedIndex(channelID string, channelType uint8, index uint64) error {
	d.calls.Add(1)
	if d.failNext.CompareAndSwap(true, false) {
		return errors.New("transient write failure")
	}
	return d.DB.UpdateChannelAppliedIndex(channelID, channelType, index)
}

type notifyingAppliedDB struct {
	wkdb.DB
	wrote chan struct{}
}

type rescanDetectingDB struct {
	wkdb.DB
	configReads   atomic.Int32
	physicalReads atomic.Int32
}

func (d *rescanDetectingDB) GetChannelClusterConfigs(uint64, int) ([]wkdb.ChannelClusterConfig, error) {
	d.configReads.Add(1)
	return nil, nil
}

func (d *rescanDetectingDB) GetChannelLastMessageSeq(string, uint8) (uint64, uint64, error) {
	d.physicalReads.Add(1)
	return 0, 0, nil
}

func (d *notifyingAppliedDB) UpdateChannelAppliedIndex(channelID string, channelType uint8, index uint64) error {
	err := d.DB.UpdateChannelAppliedIndex(channelID, channelType, index)
	select {
	case d.wrote <- struct{}{}:
	default:
	}
	return err
}
