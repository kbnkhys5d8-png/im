package wkdb

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

func TestSearchOutboxQuarantinePersistsAndRejectsLateTasks(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	message := searchOutboxTestMessage(1, 9001, true)
	appendAppliedSearchOutboxMessages(t, db, message)
	page, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	task := SearchOutboxTask{Identity: page.Records[0].Identity, Epoch: page.Records[0].Epoch}
	if task.Epoch != 1 {
		t.Fatalf("initial epoch = %d", task.Epoch)
	}
	if err := db.QuarantineSearchOutbox(task, "term_too_long"); err != nil {
		t.Fatal(err)
	}
	if err := db.QuarantineSearchOutbox(task, "term_too_long"); err != nil {
		t.Fatal(err)
	}
	requireNoRawSearchOutboxRecords(t, db, "channel", 2)
	rows, err := db.ListSearchOutboxQuarantine(10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("quarantine = %+v, %v", rows, err)
	}
	original, _ := message.Marshal()
	if !bytes.Equal(rows[0].RawValue, original) || rows[0].Epoch != 1 || rows[0].Reason != "term_too_long" {
		t.Fatalf("lost raw evidence: %+v", rows[0])
	}
	if _, err := db.LoadMsg("channel", 2, 1); err != nil {
		t.Fatalf("chat message changed: %v", err)
	}
	if err := db.AppendMessages("channel", 2, []Message{message}); err != nil {
		t.Fatal(err)
	}
	requireNoRawSearchOutboxRecords(t, db, "channel", 2)
	if err := db.AckSearchOutboxTasks([]SearchOutboxTask{task}); !errors.Is(err, ErrSearchOutboxState) {
		t.Fatalf("late ack = %v", err)
	}
	next, err := db.RestoreSearchOutbox(task)
	if err != nil || next != 2 {
		t.Fatalf("restore = %d, %v", next, err)
	}
	if next, err := db.RestoreSearchOutbox(task); err != nil || next != 2 {
		t.Fatalf("retry restore = %d, %v", next, err)
	}
	if err := db.AckSearchOutboxTasks([]SearchOutboxTask{task}); !errors.Is(err, ErrSearchOutboxStaleEpoch) {
		t.Fatalf("old epoch ack = %v", err)
	}
	if err := db.QuarantineSearchOutbox(task, "term_too_long"); !errors.Is(err, ErrSearchOutboxStaleEpoch) {
		t.Fatalf("old epoch quarantine = %v", err)
	}
	if err := db.AckSearchOutbox([]SearchOutboxIdentity{task.Identity}); !errors.Is(err, ErrSearchOutboxEpochRequired) {
		t.Fatalf("legacy ack = %v", err)
	}
	task.Epoch = next
	if err := db.AckSearchOutboxTasks([]SearchOutboxTask{task}); err != nil {
		t.Fatal(err)
	}
	if err := db.AckSearchOutboxTasks([]SearchOutboxTask{task}); err != nil {
		t.Fatal(err)
	}
	if err := db.AppendMessages("channel", 2, []Message{message}); err != nil {
		t.Fatal(err)
	}
	requireNoRawSearchOutboxRecords(t, db, "channel", 2)
}

func TestSearchOutboxQuarantineFailureKeepsActiveRecord(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	appendAppliedSearchOutboxMessages(t, db, searchOutboxTestMessage(1, 9002, true))
	page, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	task := SearchOutboxTask{Identity: page.Records[0].Identity, Epoch: page.Records[0].Epoch}
	injected := errors.New("quarantine write failed")
	installFailingPhysicalBatch(t, db, "channel", 2, key.TableSearchOutboxQuarantine.Id, injected)
	if err := db.QuarantineSearchOutbox(task, "term_too_long"); !errors.Is(err, injected) {
		t.Fatalf("quarantine = %v", err)
	}
	requireRawSearchOutboxRecordCount(t, db, "channel", 2, 1)
	rows, err := db.ListSearchOutboxQuarantine(10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("partial quarantine = %+v, %v", rows, err)
	}
}

func TestSearchOutboxQuarantinePullPassesFiveHundredCorruptRecords(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	for seq := uint32(1); seq <= 500; seq++ {
		rawKey, _ := key.NewSearchOutboxKey("channel", 2, uint64(seq), int64(10000+seq))
		if err := db.shardDBById(0).Set(rawKey, []byte("corrupt"), pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	appendAppliedSearchOutboxMessages(t, db, searchOutboxTestMessage(501, 10501, true))
	page, err := db.PullSearchOutbox(500, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	assertSearchOutboxMessageIDs(t, page.Records, 10501)
	if page.Pending != 1 {
		t.Fatalf("pending = %d", page.Pending)
	}
	rows, err := db.ListSearchOutboxQuarantine(500)
	if err != nil || len(rows) != 500 {
		t.Fatalf("quarantine count = %d, %v", len(rows), err)
	}
}

func TestSearchOutboxQuarantineConcurrentAckAndQuarantine(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	appendAppliedSearchOutboxMessages(t, db, searchOutboxTestMessage(1, 9003, true))
	page, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	task := SearchOutboxTask{Identity: page.Records[0].Identity, Epoch: page.Records[0].Epoch}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, run := range []func() error{
		func() error { return db.AckSearchOutboxTasks([]SearchOutboxTask{task}) },
		func() error { return db.QuarantineSearchOutbox(task, "bulk_item_rejected") },
	} {
		wg.Add(1)
		go func(run func() error) { defer wg.Done(); <-start; results <- run() }(run)
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		} else if !errors.Is(err, ErrSearchOutboxState) {
			t.Fatal(err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful transitions = %d", successes)
	}
	requireNoRawSearchOutboxRecords(t, db, "channel", 2)
}

func TestSearchOutboxQuarantineSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	open := func() *wukongDB {
		db := NewWukongDB(NewOptions(WithDir(dir), WithShardNum(1))).(*wukongDB)
		if err := db.Open(); err != nil {
			t.Fatal(err)
		}
		return db
	}
	db := open()
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
	})
	message := searchOutboxTestMessage(1, 9100, true)
	appendAppliedSearchOutboxMessages(t, db, message)
	task := SearchOutboxTask{Identity: searchOutboxIdentityFromMessage(message), Epoch: 1}
	if err := db.QuarantineSearchOutbox(task, "bulk_item_rejected"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db = open()
	page, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil || len(page.Records) != 0 || page.Pending != 0 || page.Quarantined != 1 {
		t.Fatalf("restarted pull = %+v, %v", page, err)
	}
	rows, err := db.ListSearchOutboxQuarantine(10)
	if err != nil || len(rows) != 1 || rows[0].Epoch != 1 {
		t.Fatalf("restarted quarantine = %+v, %v", rows, err)
	}
	if err := db.AppendMessages("channel", 2, []Message{message}); err != nil {
		t.Fatal(err)
	}
	requireNoRawSearchOutboxRecords(t, db, "channel", 2)
	if epoch, err := db.RestoreSearchOutbox(task); err != nil || epoch != 2 {
		t.Fatalf("restore = %d, %v", epoch, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
	db = open()
	page, err = db.PullSearchOutbox(10, 1<<20)
	if err != nil || len(page.Records) != 1 || page.Records[0].Epoch != 2 || page.Quarantined != 0 {
		t.Fatalf("restarted epoch = %+v, %v", page, err)
	}
	if err := db.AckSearchOutboxTasks([]SearchOutboxTask{task}); !errors.Is(err, ErrSearchOutboxStaleEpoch) {
		t.Fatalf("restarted late ack = %v", err)
	}
}

func TestSearchOutboxQuarantineSmallBudgetDoesNotIsolateHealthyTasks(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	large := searchOutboxTestMessage(1, 9201, true)
	large.Payload = bytes.Repeat([]byte("x"), 2048)
	small := searchOutboxTestMessage(2, 9202, true)
	appendAppliedSearchOutboxMessages(t, db, large, small)
	page, err := db.PullSearchOutbox(500, 512)
	if err != nil {
		t.Fatal(err)
	}
	assertSearchOutboxMessageIDs(t, page.Records, 9202)
	if page.Pending != 2 {
		t.Fatalf("pending = %d", page.Pending)
	}
	rows, err := db.ListSearchOutboxQuarantine(10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("small budget quarantined healthy tasks: %+v, %v", rows, err)
	}
	page, err = db.PullSearchOutbox(500, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	assertSearchOutboxMessageIDs(t, page.Records, 9201, 9202)
	if page.Records[0].Epoch != 1 {
		t.Fatalf("healthy epoch changed = %d", page.Records[0].Epoch)
	}
}

func TestSearchOutboxQuarantineAbsoluteOversizeDoesNotOccupyBudget(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	large := searchOutboxTestMessage(1, 9301, true)
	large.Payload = bytes.Repeat([]byte("x"), int(MaxSearchOutboxRecordBytes))
	small := searchOutboxTestMessage(2, 9302, true)
	appendAppliedSearchOutboxMessages(t, db, large, small)
	page, err := db.PullSearchOutbox(1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	assertSearchOutboxMessageIDs(t, page.Records, 9302)
	if page.Pending != 1 {
		t.Fatalf("oversize occupies pending = %d", page.Pending)
	}
	rows, err := db.ListSearchOutboxQuarantine(10)
	if err != nil || len(rows) != 1 {
		t.Fatalf("oversize quarantine count = %d, %v", len(rows), err)
	}
	if rows[0].Reason != "source_oversize" {
		t.Fatalf("oversize quarantine reason = %s", rows[0].Reason)
	}
	original, _ := large.Marshal()
	if !bytes.Equal(rows[0].RawValue, original) {
		t.Fatal("oversize evidence differs")
	}
}

func TestSearchOutboxQuarantineFiveHundredRejectedTasksLeaveNextPageReachable(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	messages := make([]Message, 501)
	for i := range messages {
		messages[i] = searchOutboxTestMessage(uint32(i+1), int64(9400+i), true)
	}
	if err := db.AppendMessages("channel", 2, messages); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateChannelAppliedIndex("channel", 2, 501); err != nil {
		t.Fatal(err)
	}
	page, err := db.PullSearchOutbox(500, 1<<20)
	if err != nil || len(page.Records) != 500 {
		t.Fatalf("first page = %d, %v", len(page.Records), err)
	}
	for _, record := range page.Records {
		if err := db.QuarantineSearchOutbox(SearchOutboxTask{Identity: record.Identity, Epoch: record.Epoch}, "term_too_long"); err != nil {
			t.Fatal(err)
		}
	}
	page, err = db.PullSearchOutbox(500, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	assertSearchOutboxMessageIDs(t, page.Records, 9900)
	if err := db.AckSearchOutboxTasks([]SearchOutboxTask{{Identity: page.Records[0].Identity, Epoch: page.Records[0].Epoch}}); err != nil {
		t.Fatal(err)
	}
	requireNoRawSearchOutboxRecords(t, db, "channel", 2)
}

func TestSearchOutboxQuarantineStaleScanCannotDeleteNewValue(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	message := searchOutboxTestMessage(1, 9501, true)
	appendAppliedSearchOutboxMessages(t, db, message)
	rawKey, value := requireSingleRawSearchOutboxEntry(t, db, "channel", 2)
	if removed, err := db.quarantineSearchOutboxCandidate(0, rawKey, []byte("old-corrupt-value"), "invalid_record"); err != nil || removed {
		t.Fatalf("stale candidate removal = %v, %v", removed, err)
	}
	current, err := db.loadSearchOutboxBytes(0, rawKey)
	if err != nil || !bytes.Equal(current, value) {
		t.Fatalf("stale snapshot changed source: %v", err)
	}
	rows, err := db.ListSearchOutboxQuarantine(10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("stale snapshot isolated new value: %+v, %v", rows, err)
	}
}

func TestSearchOutboxQuarantineStartupSkipsMalformedKeysAndReleasesLockBeforeCallback(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	badKey := append(key.NewSearchOutboxLowKey(), byte(2))
	if err := db.shardDBById(0).Set(badKey, []byte("raw evidence"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	appendAppliedSearchOutboxMessages(t, db, searchOutboxTestMessage(1, 9601, true))
	visited := 0
	if err := db.ScanSearchOutboxChannels(context.Background(), func(channel Channel) error {
		visited++
		// 回调模拟启动恢复再次访问存储，持锁回调会死锁。
		return db.AppendMessages(channel.ChannelId, channel.ChannelType, []Message{searchOutboxTestMessage(2, 9602, true)})
	}); err != nil {
		t.Fatal(err)
	}
	if visited != 1 {
		t.Fatalf("visited = %d", visited)
	}
	rows, err := db.ListSearchOutboxQuarantine(10)
	if err != nil || len(rows) != 1 || !bytes.Equal(rows[0].RawKey, badKey) {
		t.Fatalf("startup quarantine = %+v, %v", rows, err)
	}
}

type searchOutboxCommitErrorBatch struct {
	physicalBatch
	err         error
	afterCommit bool
}

func (batch *searchOutboxCommitErrorBatch) Commit(options *pebble.WriteOptions) error {
	if batch.afterCommit {
		if err := batch.physicalBatch.Commit(options); err != nil {
			return err
		}
	}
	return batch.err
}

func TestSearchOutboxQuarantineCommitFailureAndUnknownOutcome(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		name := "commit_rejected"
		if afterCommit {
			name = "committed_response_lost"
		}
		t.Run(name, func(t *testing.T) {
			db := openSearchOutboxTestDB(t)
			message := searchOutboxTestMessage(1, 9701, true)
			appendAppliedSearchOutboxMessages(t, db, message)
			task := SearchOutboxTask{Identity: searchOutboxIdentityFromMessage(message), Epoch: 1}
			batchDB := db.shardBatchDBById(0)
			original := batchDB.newPhysicalBatch
			injected := errors.New("commit result unavailable")
			batchDB.newPhysicalBatch = func() physicalBatch {
				return &searchOutboxCommitErrorBatch{physicalBatch: original(), err: injected, afterCommit: afterCommit}
			}
			err := db.QuarantineSearchOutbox(task, "term_too_long")
			batchDB.newPhysicalBatch = original
			if !errors.Is(err, injected) {
				t.Fatalf("quarantine = %v", err)
			}
			rows, err := db.ListSearchOutboxQuarantine(10)
			if err != nil {
				t.Fatal(err)
			}
			if afterCommit {
				if len(rows) != 1 {
					t.Fatal("commit lost quarantine evidence")
				}
				requireNoRawSearchOutboxRecords(t, db, "channel", 2)
			} else {
				if len(rows) != 0 {
					t.Fatal("failed commit left partial quarantine")
				}
				requireRawSearchOutboxRecordCount(t, db, "channel", 2, 1)
			}
			if err := db.QuarantineSearchOutbox(task, "term_too_long"); err != nil {
				t.Fatalf("retry = %v", err)
			}
			rows, err = db.ListSearchOutboxQuarantine(10)
			if err != nil || len(rows) != 1 {
				t.Fatalf("retry evidence = %+v, %v", rows, err)
			}
		})
	}
}

func TestSearchOutboxQuarantineRejectsUnsafeReasonAndMixedEpochs(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	message := searchOutboxTestMessage(1, 9801, true)
	appendAppliedSearchOutboxMessages(t, db, message)
	task := SearchOutboxTask{Identity: searchOutboxIdentityFromMessage(message), Epoch: 1}
	if err := db.QuarantineSearchOutbox(task, "secret chat body"); err == nil {
		t.Fatal("accepted untrusted reason")
	}
	newer := task
	newer.Epoch++
	if err := db.AckSearchOutboxTasks([]SearchOutboxTask{task, newer}); !errors.Is(err, ErrSearchOutboxStaleEpoch) {
		t.Fatalf("mixed epochs = %v", err)
	}
	requireRawSearchOutboxRecordCount(t, db, "channel", 2, 1)
}

func TestSearchOutboxQuarantineRestoreFailureKeepsOneDurableOwner(t *testing.T) {
	for _, afterCommit := range []bool{false, true} {
		name := "commit_rejected"
		if afterCommit {
			name = "committed_response_lost"
		}
		t.Run(name, func(t *testing.T) {
			db := openSearchOutboxTestDB(t)
			message := searchOutboxTestMessage(1, 9901, true)
			appendAppliedSearchOutboxMessages(t, db, message)
			task := SearchOutboxTask{Identity: searchOutboxIdentityFromMessage(message), Epoch: 1}
			if err := db.QuarantineSearchOutbox(task, "term_too_long"); err != nil {
				t.Fatal(err)
			}
			batchDB := db.shardBatchDBById(0)
			original := batchDB.newPhysicalBatch
			injected := errors.New("restore commit result unavailable")
			batchDB.newPhysicalBatch = func() physicalBatch {
				return &searchOutboxCommitErrorBatch{physicalBatch: original(), err: injected, afterCommit: afterCommit}
			}
			_, err := db.RestoreSearchOutbox(task)
			batchDB.newPhysicalBatch = original
			if !errors.Is(err, injected) {
				t.Fatalf("restore = %v", err)
			}
			rows, err := db.ListSearchOutboxQuarantine(10)
			if err != nil {
				t.Fatal(err)
			}
			if afterCommit {
				if len(rows) != 0 {
					t.Fatal("committed restore retained quarantine owner")
				}
				requireRawSearchOutboxRecordCount(t, db, "channel", 2, 1)
			} else {
				if len(rows) != 1 {
					t.Fatal("failed restore lost quarantine owner")
				}
				requireNoRawSearchOutboxRecords(t, db, "channel", 2)
			}
			if epoch, err := db.RestoreSearchOutbox(task); err != nil || epoch != 2 {
				t.Fatalf("restore retry = %d, %v", epoch, err)
			}
		})
	}
}

func TestSearchOutboxQuarantineRestoreWinsAgainstLatePriorEpochRequests(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	messages := make([]Message, 20)
	for i := range messages {
		messages[i] = searchOutboxTestMessage(uint32(i+1), int64(11000+i), true)
	}
	if err := db.AppendMessages("channel", 2, messages); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateChannelAppliedIndex("channel", 2, 20); err != nil {
		t.Fatal(err)
	}
	for _, message := range messages {
		task := SearchOutboxTask{Identity: searchOutboxIdentityFromMessage(message), Epoch: 1}
		if err := db.QuarantineSearchOutbox(task, "term_too_long"); err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		results := make(chan error, 3)
		go func() { <-start; _, err := db.RestoreSearchOutbox(task); results <- err }()
		go func() { <-start; results <- db.AckSearchOutboxTasks([]SearchOutboxTask{task}) }()
		go func() { <-start; results <- db.QuarantineSearchOutbox(task, "term_too_long") }()
		close(start)
		for i := 0; i < 3; i++ {
			err := <-results
			if err != nil && !errors.Is(err, ErrSearchOutboxState) && !errors.Is(err, ErrSearchOutboxStaleEpoch) {
				t.Fatal(err)
			}
		}
		rawKey, _ := task.key()
		state, _, err := db.loadSearchOutboxState(0, rawKey)
		if err != nil || state.Epoch != 2 || state.Status != searchOutboxActive {
			t.Fatalf("late request changed restored task: %+v, %v", state, err)
		}
		task.Epoch = 2
		if err := db.AckSearchOutboxTasks([]SearchOutboxTask{task}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSearchOutboxQuarantineCorruptStateCannotDiscardSource(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	message := searchOutboxTestMessage(1, 12001, true)
	appendAppliedSearchOutboxMessages(t, db, message)
	task := SearchOutboxTask{Identity: searchOutboxIdentityFromMessage(message), Epoch: 1}
	rawKey, _ := task.key()
	if err := db.shardDBById(0).Set(key.NewSearchOutboxStateKey(rawKey), []byte("broken state"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := db.QuarantineSearchOutbox(task, "term_too_long"); err == nil {
		t.Fatal("quarantine accepted corrupt state")
	}
	if _, err := db.PullSearchOutbox(10, 1<<20); err == nil {
		t.Fatal("pull accepted corrupt state")
	}
	requireRawSearchOutboxRecordCount(t, db, "channel", 2, 1)
	rows, err := db.ListSearchOutboxQuarantine(10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("corrupt state changed quarantine count = %d, %v", len(rows), err)
	}
}

type searchOutboxBlockedCommitBatch struct {
	physicalBatch
	entered chan struct{}
	release <-chan struct{}
}

func (batch *searchOutboxBlockedCommitBatch) Commit(options *pebble.WriteOptions) error {
	close(batch.entered)
	<-batch.release
	return batch.physicalBatch.Commit(options)
}

func TestSearchOutboxQuarantineSlowShardDoesNotBlockOtherShardAppendOrPull(t *testing.T) {
	db := openSearchOutboxTestDBWithShards(t, 2)
	first := searchOutboxTestMessage(1, 13001, true)
	first.ChannelID = findSearchOutboxChannelOnShard(t, db, 0)
	second := searchOutboxTestMessage(1, 13002, true)
	second.ChannelID = findSearchOutboxChannelOnShard(t, db, 1)
	appendAppliedSearchOutboxMessages(t, db, first)
	entered := make(chan struct{})
	release := make(chan struct{})
	batchDB := db.shardBatchDBById(0)
	original := batchDB.newPhysicalBatch
	batchDB.newPhysicalBatch = func() physicalBatch {
		return &searchOutboxBlockedCommitBatch{physicalBatch: original(), entered: entered, release: release}
	}
	quarantineDone := make(chan error, 1)
	go func() {
		quarantineDone <- db.QuarantineSearchOutbox(SearchOutboxTask{Identity: searchOutboxIdentityFromMessage(first), Epoch: 1}, "term_too_long")
	}()
	defer func() {
		close(release)
		if err := <-quarantineDone; err != nil {
			t.Errorf("blocked quarantine = %v", err)
		}
		batchDB.newPhysicalBatch = original
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first shard never reached blocked commit")
	}
	appendDone := make(chan error, 1)
	go func() { appendDone <- db.AppendMessages(second.ChannelID, second.ChannelType, []Message{second}) }()
	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow shard blocked another shard's chat append")
	}
	if err := db.UpdateChannelAppliedIndex(second.ChannelID, second.ChannelType, 1); err != nil {
		t.Fatal(err)
	}
	type pullResult struct {
		page SearchOutboxPullResult
		err  error
	}
	pullDone := make(chan pullResult, 1)
	go func() { page, err := db.PullSearchOutbox(500, 1<<20); pullDone <- pullResult{page, err} }()
	select {
	case result := <-pullDone:
		if result.err != nil {
			t.Fatal(result.err)
		}
		assertSearchOutboxMessageIDs(t, result.page.Records, second.MessageID)
	case <-time.After(2 * time.Second):
		t.Fatal("slow shard blocked another shard's search pull")
	}
}

func TestSearchOutboxQuarantineBusyCorruptShardRemainsPendingWithUnknownOldest(t *testing.T) {
	db := openSearchOutboxTestDBWithShards(t, 2)
	bad := searchOutboxTestMessage(1, 13501, true)
	bad.ChannelID = findSearchOutboxChannelOnShard(t, db, 0)
	good := searchOutboxTestMessage(1, 13502, true)
	good.ChannelID = findSearchOutboxChannelOnShard(t, db, 1)
	appendAppliedSearchOutboxMessages(t, db, bad, good)
	task := SearchOutboxTask{Identity: searchOutboxIdentityFromMessage(bad), Epoch: 1}
	rawKey, _ := task.key()
	if err := db.shardDBById(0).Set(rawKey, []byte("corrupt without timestamp"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	batchDB := db.shardBatchDBById(0)
	original := batchDB.newPhysicalBatch
	batchDB.newPhysicalBatch = func() physicalBatch {
		return &searchOutboxBlockedCommitBatch{physicalBatch: original(), entered: entered, release: release}
	}
	done := make(chan error, 1)
	go func() { done <- db.QuarantineSearchOutbox(task, "invalid_record") }()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Errorf("blocked quarantine = %v", err)
		}
		batchDB.newPhysicalBatch = original
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first shard never reached blocked commit")
	}
	page, err := db.PullSearchOutbox(500, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	assertSearchOutboxMessageIDs(t, page.Records, good.MessageID)
	if page.Pending != 2 || page.Quarantined != 0 || page.OldestCreatedAt != 0 {
		t.Fatalf("busy corrupt task hidden from backlog: pending=%d quarantined=%d oldest=%d", page.Pending, page.Quarantined, page.OldestCreatedAt)
	}
	goodTask := SearchOutboxTask{Identity: page.Records[0].Identity, Epoch: page.Records[0].Epoch}
	if err := db.AckSearchOutboxTasks([]SearchOutboxTask{goodTask}); err != nil {
		t.Fatal(err)
	}
	page, err = db.PullSearchOutbox(500, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Records) != 0 || page.Pending != 1 || page.Quarantined != 0 || page.OldestCreatedAt != 0 {
		t.Fatalf("only busy corrupt task reported as empty: records=%d pending=%d quarantined=%d oldest=%d", len(page.Records), page.Pending, page.Quarantined, page.OldestCreatedAt)
	}
}

func TestSearchOutboxQuarantineRestoreRejectsTruncatedOrReplacedMessage(t *testing.T) {
	for _, name := range []string{"truncated", "replaced"} {
		t.Run(name, func(t *testing.T) {
			db := openSearchOutboxTestDB(t)
			first := searchOutboxTestMessage(1, 14001, true)
			message := searchOutboxTestMessage(2, 14002, true)
			appendAppliedSearchOutboxMessages(t, db, first, message)
			task := SearchOutboxTask{Identity: searchOutboxIdentityFromMessage(message), Epoch: 1}
			if err := db.QuarantineSearchOutbox(task, "term_too_long"); err != nil {
				t.Fatal(err)
			}
			if name == "truncated" {
				if err := db.TruncateLogTo("channel", 2, 1); err != nil {
					t.Fatal(err)
				}
			} else {
				replacement := message
				replacement.MessageID = 14003
				if err := db.AppendMessages("channel", 2, []Message{replacement}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.RestoreSearchOutbox(task); err == nil {
				t.Fatal("restore resurrected obsolete message")
			}
			rows, err := db.ListSearchOutboxQuarantine(10)
			if err != nil || len(rows) != 1 || rows[0].Epoch != 1 {
				t.Fatalf("obsolete task evidence count = %d, %v", len(rows), err)
			}
			rawKey, _ := task.key()
			if _, err := db.loadSearchOutboxBytes(0, rawKey); !errors.Is(err, pebble.ErrNotFound) {
				t.Fatalf("obsolete task restored = %v", err)
			}
			current, err := db.LoadMsg("channel", 2, 2)
			if name == "truncated" {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("truncated chat changed = %v", err)
				}
			} else if err != nil || current.MessageID != 14003 {
				t.Fatalf("replacement chat changed: id=%d err=%v", current.MessageID, err)
			}
		})
	}
}
