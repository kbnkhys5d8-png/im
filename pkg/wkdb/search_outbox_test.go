package wkdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
)

func TestAppendMessagesWritesCompleteOutboxValue(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	message := searchOutboxTestMessage(1, 101, true)
	if err := db.AppendMessages("channel", 2, []Message{message}); err != nil {
		t.Fatal(err)
	}
	record := requireSingleRawSearchOutboxRecord(t, db, "channel", 2)
	if record.Identity != (SearchOutboxIdentity{
		ChannelID: "channel", ChannelType: 2,
		MessageSeq: 1, MessageID: 101,
	}) {
		t.Fatalf("identity = %+v", record.Identity)
	}
	if !record.Message.SearchOutbox || !bytes.Equal(record.Message.Payload, message.Payload) || record.Message.ClientMsgNo != message.ClientMsgNo {
		t.Fatalf("outbox value = %+v, want complete message", record.Message)
	}
}

func TestAppendMessagesWithoutFlagDoesNotWriteOutbox(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	message := searchOutboxTestMessage(1, 102, false)
	if err := db.AppendMessages("channel", 2, []Message{message}); err != nil {
		t.Fatal(err)
	}
	requireNoRawSearchOutboxRecords(t, db, "channel", 2)
}

func TestAppendMessagesOutboxOperationFailureCommitsNoPrefix(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	injected := errors.New("outbox set failed")
	physical := installFailingPhysicalBatch(t, db, "channel", 2, key.TableSearchOutbox.Id, injected)

	err := db.AppendMessages("channel", 2, []Message{searchOutboxTestMessage(1, 103, true)})
	if !errors.Is(err, injected) {
		t.Fatalf("AppendMessages error = %v, want %v", err, injected)
	}
	requireMessageMissing(t, db, "channel", 2, 1)
	requireNoRawSearchOutboxRecords(t, db, "channel", 2)
	seq, _, err := db.GetChannelLastMessageSeq("channel", 2)
	if err != nil {
		t.Fatal(err)
	}
	if seq != 0 {
		t.Fatalf("last sequence = %d, want 0", seq)
	}
	if physical.commitCalls != 0 {
		t.Fatalf("physical Commit calls = %d, want 0", physical.commitCalls)
	}
}

func TestAppendMessagesWritesSearchOutboxFloorOnce(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	if err := db.AppendMessages("channel", 2, []Message{searchOutboxTestMessage(41, 141, true)}); err != nil {
		t.Fatal(err)
	}
	floor, enabled, err := db.GetSearchOutboxFloor("channel", 2)
	if err != nil || !enabled || floor != 40 {
		t.Fatalf("first floor = %d/%v, err=%v", floor, enabled, err)
	}
	if err := db.AppendMessages("channel", 2, []Message{searchOutboxTestMessage(42, 142, true)}); err != nil {
		t.Fatal(err)
	}
	floor, enabled, err = db.GetSearchOutboxFloor("channel", 2)
	if err != nil || !enabled || floor != 40 {
		t.Fatalf("stable floor = %d/%v, err=%v", floor, enabled, err)
	}
}

func TestAppendMessagesRejectsMismatchedMessageChannelBeforeBatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *wukongDB, *Message)
	}{
		{
			name: "different channel id across shard",
			mutate: func(t *testing.T, db *wukongDB, message *Message) {
				message.ChannelID = findCrossShardChannelID(t, db, message.ChannelID, message.ChannelType)
			},
		},
		{
			name: "different channel type",
			mutate: func(_ *testing.T, _ *wukongDB, message *Message) {
				message.ChannelType = 3
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openSearchOutboxTestDBWithShards(t, 2)
			const targetChannelID = "channel"
			const targetChannelType uint8 = 2
			message := searchOutboxTestMessage(1, 151, true)
			test.mutate(t, db, &message)

			if err := db.AppendMessages(targetChannelID, targetChannelType, []Message{message}); err == nil {
				t.Error("AppendMessages accepted a mismatched message channel identity")
			}
			requireSearchOutboxChannelStateEmpty(t, db, targetChannelID, targetChannelType, 1)
			requireSearchOutboxChannelStateEmpty(t, db, message.ChannelID, message.ChannelType, 1)
		})
	}
}

func TestPullSearchOutboxBlocksBeyondDurableAppliedIndex(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	appendSearchOutboxMessages(t, db,
		searchOutboxTestMessage(1, 201, true),
		searchOutboxTestMessage(2, 202, true),
	)
	if err := db.UpdateChannelAppliedIndex("channel", 2, 1); err != nil {
		t.Fatal(err)
	}

	first, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != 1 ||
		first.Records[0].Identity.MessageSeq != 1 ||
		first.Records[0].AppliedIndex != 1 ||
		first.Pending != 2 ||
		first.AppliedBlocked != 1 {
		t.Fatalf("first pull = %+v", first)
	}

	if err := db.UpdateChannelAppliedIndex("channel", 2, 2); err != nil {
		t.Fatal(err)
	}
	second, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 2 ||
		second.Records[0].AppliedIndex != 2 ||
		second.Records[1].AppliedIndex != 2 ||
		second.Pending != 2 ||
		second.AppliedBlocked != 0 {
		t.Fatalf("second pull = %+v", second)
	}
}

func TestPullSearchOutboxUsesStoredValueAfterMessageRowIsAbsent(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	message := searchOutboxTestMessage(1, 203, true)
	appendSearchOutboxMessages(t, db, message)
	if err := db.UpdateChannelAppliedIndex("channel", 2, 1); err != nil {
		t.Fatal(err)
	}
	deleteOnlyMessageColumns(t, db, "channel", 2, 1)

	result, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 ||
		!bytes.Equal(result.Records[0].Message.Payload, message.Payload) {
		t.Fatalf("pull = %+v, want stored outbox value", result)
	}
}

func TestPullSearchOutboxMissingAppliedStateIsBlocked(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	appendSearchOutboxMessages(t, db, searchOutboxTestMessage(1, 204, true))
	result, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 0 ||
		result.Pending != 1 ||
		result.AppliedBlocked != 1 {
		t.Fatalf("pull = %+v, want one blocked record", result)
	}
}

func TestPullSearchOutboxRejectsInvalidBounds(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	tests := []struct {
		name     string
		limit    int
		maxBytes uint64
	}{
		{name: "zero limit", limit: 0, maxBytes: 1},
		{name: "limit above maximum", limit: MaxSearchOutboxPullLimit + 1, maxBytes: 1},
		{name: "zero byte budget", limit: 1, maxBytes: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := db.PullSearchOutbox(test.limit, test.maxBytes); err == nil {
				t.Fatal("PullSearchOutbox accepted invalid bounds")
			}
		})
	}
}

func TestPullSearchOutboxCountsAllPendingBeyondRecordLimit(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	first := searchOutboxTestMessage(1, 205, true)
	first.Timestamp = 300
	second := searchOutboxTestMessage(2, 206, true)
	second.Timestamp = 100
	appendSearchOutboxMessages(t, db, first, second)
	if err := db.UpdateChannelAppliedIndex("channel", 2, 2); err != nil {
		t.Fatal(err)
	}

	result, err := db.PullSearchOutbox(1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 ||
		result.Records[0].Identity.MessageSeq != 1 ||
		result.Pending != 2 ||
		result.OldestCreatedAt != 100 {
		t.Fatalf("limited pull = %+v", result)
	}
}

func TestPullSearchOutboxByteBudgetLeavesOversizedRecordPending(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	message := searchOutboxTestMessage(1, 207, true)
	message.Payload = bytes.Repeat([]byte("x"), 128)
	appendSearchOutboxMessages(t, db, message)
	if err := db.UpdateChannelAppliedIndex("channel", 2, 1); err != nil {
		t.Fatal(err)
	}
	keyBytes, value := requireSingleRawSearchOutboxEntry(t, db, "channel", 2)
	recordBytes := uint64(len(keyBytes) + len(value))
	if recordBytes < 2 {
		t.Fatalf("raw record size = %d", recordBytes)
	}

	if _, err := db.PullSearchOutbox(10, recordBytes-1); !errors.Is(err, ErrSearchOutboxByteBudget) {
		t.Fatalf("undersized budget error = %v, want ErrSearchOutboxByteBudget", err)
	}
	result, err := db.PullSearchOutbox(10, recordBytes)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 1 || result.Pending != 1 {
		t.Fatalf("retry pull = %+v, want the still-pending record", result)
	}
}

func TestPullSearchOutboxIsStableWithoutAck(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	appendSearchOutboxMessages(t, db,
		searchOutboxTestMessage(1, 208, true),
		searchOutboxTestMessage(2, 209, true),
	)
	if err := db.UpdateChannelAppliedIndex("channel", 2, 2); err != nil {
		t.Fatal(err)
	}
	first, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	second, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != 2 || len(second.Records) != 2 ||
		first.Records[0].Identity != second.Records[0].Identity ||
		first.Records[1].Identity != second.Records[1].Identity ||
		first.Pending != second.Pending {
		t.Fatalf("repeat pulls differ: first=%+v second=%+v", first, second)
	}
}

func TestPullSearchOutboxOrdersShardIDsAscending(t *testing.T) {
	db := openSearchOutboxTestDBWithShards(t, 2)
	channel0 := findSearchOutboxChannelOnShard(t, db, 0)
	channel1 := findSearchOutboxChannelOnShard(t, db, 1)
	first := searchOutboxTestMessage(1, 210, true)
	first.ChannelID = channel0
	second := searchOutboxTestMessage(1, 211, true)
	second.ChannelID = channel1
	appendSearchOutboxMessages(t, db, first, second)
	if err := db.UpdateChannelAppliedIndex(channel0, 2, 1); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateChannelAppliedIndex(channel1, 2, 1); err != nil {
		t.Fatal(err)
	}

	result, err := db.PullSearchOutbox(10, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Records) != 2 ||
		result.Records[0].Identity.ChannelID != channel0 ||
		result.Records[1].Identity.ChannelID != channel1 {
		t.Fatalf("cross-shard order = %+v, want shard 0 then shard 1", result.Records)
	}
}

func TestPullSearchOutboxRejectsCorruptKeyOrValue(t *testing.T) {
	t.Run("key", func(t *testing.T) {
		db := openSearchOutboxTestDB(t)
		corruptKey := append(key.NewSearchOutboxLowKey(), byte(2))
		if err := db.shardDBById(0).Set(corruptKey, []byte("value"), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, err := db.PullSearchOutbox(10, 1<<20); err == nil {
			t.Fatal("PullSearchOutbox accepted a corrupt key")
		}
	})

	t.Run("value", func(t *testing.T) {
		db := openSearchOutboxTestDB(t)
		appendSearchOutboxMessages(t, db, searchOutboxTestMessage(1, 212, true))
		keyBytes, _ := requireSingleRawSearchOutboxEntry(t, db, "channel", 2)
		if err := db.shardDBById(0).Set(keyBytes, []byte("corrupt"), pebble.Sync); err != nil {
			t.Fatal(err)
		}
		if _, err := db.PullSearchOutbox(10, 1<<20); err == nil {
			t.Fatal("PullSearchOutbox accepted a corrupt value")
		}
	})
}

func TestPullSearchOutboxRejectsMissingServerTimestamp(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	message := searchOutboxTestMessage(1, 213, true)
	message.Timestamp = 0
	appendSearchOutboxMessages(t, db, message)
	if err := db.UpdateChannelAppliedIndex("channel", 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.PullSearchOutbox(10, 1<<20); err == nil {
		t.Fatal("PullSearchOutbox accepted a pending record without a server timestamp")
	}
}

func TestScanSearchOutboxChannelsVisitsOnlyDistinctPendingChannels(t *testing.T) {
	db := openSearchOutboxTestDB(t)
	first := searchOutboxTestMessage(1, 214, true)
	first.ChannelID = "a"
	second := searchOutboxTestMessage(2, 215, true)
	second.ChannelID = "a"
	third := searchOutboxTestMessage(1, 216, true)
	third.ChannelID = "b"
	legacy := searchOutboxTestMessage(1, 217, false)
	legacy.ChannelID = "legacy"
	appendSearchOutboxMessages(t, db, first, second, third, legacy)
	deleteOnlyMessageColumns(t, db, "a", 2, 1)
	deleteOnlyMessageColumns(t, db, "a", 2, 2)
	deleteOnlyMessageColumns(t, db, "b", 2, 1)

	var visited []Channel
	if err := db.ScanSearchOutboxChannels(context.Background(), func(channel Channel) error {
		visited = append(visited, channel)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []Channel{
		{ChannelId: "a", ChannelType: 2},
		{ChannelId: "b", ChannelType: 2},
	}
	if len(visited) != len(want) || visited[0] != want[0] || visited[1] != want[1] {
		t.Fatalf("visited = %+v, want %+v", visited, want)
	}
}

func TestScanSearchOutboxChannelsHonorsCancellationAndVisitErrors(t *testing.T) {
	t.Run("canceled", func(t *testing.T) {
		db := openSearchOutboxTestDB(t)
		appendSearchOutboxMessages(t, db, searchOutboxTestMessage(1, 218, true))
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := db.ScanSearchOutboxChannels(ctx, func(Channel) error {
			t.Fatal("visit called after cancellation")
			return nil
		}); !errors.Is(err, context.Canceled) {
			t.Fatalf("ScanSearchOutboxChannels error = %v, want context.Canceled", err)
		}
	})

	t.Run("visit error", func(t *testing.T) {
		db := openSearchOutboxTestDB(t)
		appendSearchOutboxMessages(t, db, searchOutboxTestMessage(1, 219, true))
		injected := errors.New("visit failed")
		err := db.ScanSearchOutboxChannels(context.Background(), func(Channel) error {
			return injected
		})
		if !errors.Is(err, injected) {
			t.Fatalf("ScanSearchOutboxChannels error = %v, want %v", err, injected)
		}
	})
}

type outboxFailingPhysicalBatch struct {
	failTable   [2]byte
	err         error
	commitCalls int
}

func (b *outboxFailingPhysicalBatch) Set(keyBytes, _ []byte, _ *pebble.WriteOptions) error {
	if len(keyBytes) >= len(b.failTable) && bytes.Equal(keyBytes[:len(b.failTable)], b.failTable[:]) {
		return b.err
	}
	return nil
}

func (*outboxFailingPhysicalBatch) Delete([]byte, *pebble.WriteOptions) error { return nil }

func (*outboxFailingPhysicalBatch) DeleteRange([]byte, []byte, *pebble.WriteOptions) error {
	return nil
}

func (b *outboxFailingPhysicalBatch) Commit(*pebble.WriteOptions) error {
	b.commitCalls++
	return nil
}

func (*outboxFailingPhysicalBatch) Close() error { return nil }

func installFailingPhysicalBatch(t *testing.T, db *wukongDB, channelID string, channelType uint8, failTable [2]byte, err error) *outboxFailingPhysicalBatch {
	t.Helper()
	batchDB := db.channelBatchDb(channelID, channelType)
	original := batchDB.newPhysicalBatch
	physical := &outboxFailingPhysicalBatch{failTable: failTable, err: err}
	batchDB.newPhysicalBatch = func() physicalBatch { return physical }
	t.Cleanup(func() {
		batchDB.newPhysicalBatch = original
	})
	return physical
}

func openSearchOutboxTestDB(t *testing.T) *wukongDB {
	return openSearchOutboxTestDBWithShards(t, 1)
}

func openSearchOutboxTestDBWithShards(t *testing.T, shardCount int) *wukongDB {
	t.Helper()
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(shardCount))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return db
}

func findCrossShardChannelID(t *testing.T, db *wukongDB, channelID string, channelType uint8) string {
	t.Helper()
	targetShard := db.channelDbIndex(channelID, channelType)
	for index := 0; index < 1000; index++ {
		candidate := fmt.Sprintf("%s-mismatch-%d", channelID, index)
		if db.channelDbIndex(candidate, channelType) != targetShard {
			return candidate
		}
	}
	t.Fatal("could not find a channel identity on another shard")
	return ""
}

func searchOutboxTestMessage(sequence uint32, messageID int64, searchOutbox bool) Message {
	return Message{
		RecvPacket: wkproto.RecvPacket{
			MessageID: messageID, MessageSeq: sequence, ClientMsgNo: "client",
			ChannelID: "channel", ChannelType: 2, FromUID: "sender",
			Timestamp: int32(1000 + sequence), Payload: []byte("body"),
		},
		Term: 6, SearchOutbox: searchOutbox,
	}
}

func appendSearchOutboxMessages(t *testing.T, db *wukongDB, messages ...Message) {
	t.Helper()
	for _, message := range messages {
		if err := db.AppendMessages(
			message.ChannelID,
			message.ChannelType,
			[]Message{message},
		); err != nil {
			t.Fatal(err)
		}
	}
}

func deleteOnlyMessageColumns(t *testing.T, db *wukongDB, channelID string, channelType uint8, messageSeq uint64) {
	t.Helper()
	if err := db.channelDb(channelID, channelType).DeleteRange(
		key.NewMessagePrimaryKey(channelID, channelType, messageSeq),
		key.NewMessagePrimaryKey(channelID, channelType, messageSeq+1),
		pebble.Sync,
	); err != nil {
		t.Fatal(err)
	}
}

func findSearchOutboxChannelOnShard(t *testing.T, db *wukongDB, shard uint32) string {
	t.Helper()
	for index := 0; index < 1000; index++ {
		candidate := fmt.Sprintf("search-outbox-shard-%d-%d", shard, index)
		if db.channelDbIndex(candidate, 2) == shard {
			return candidate
		}
	}
	t.Fatalf("could not find a channel on shard %d", shard)
	return ""
}

func requireSingleRawSearchOutboxEntry(t *testing.T, db *wukongDB, channelID string, channelType uint8) ([]byte, []byte) {
	t.Helper()
	lower, upper, err := key.NewSearchOutboxChannelRange(channelID, channelType, 1)
	if err != nil {
		t.Fatal(err)
	}
	iter := db.channelDb(channelID, channelType).NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	defer iter.Close()
	if !iter.First() {
		t.Fatal("outbox record is missing")
	}
	keyBytes := append([]byte(nil), iter.Key()...)
	value := append([]byte(nil), iter.Value()...)
	if iter.Next() {
		t.Fatal("multiple outbox records")
	}
	return keyBytes, value
}

func requireSingleRawSearchOutboxRecord(t *testing.T, db *wukongDB, channelID string, channelType uint8) SearchOutboxRecord {
	t.Helper()
	lower, upper, err := key.NewSearchOutboxChannelRange(channelID, channelType, 1)
	if err != nil {
		t.Fatal(err)
	}
	iter := db.channelDb(channelID, channelType).NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	defer iter.Close()
	if !iter.First() {
		t.Fatal("outbox record is missing")
	}
	record, err := decodeSearchOutboxRecord(iter.Key(), iter.Value())
	if err != nil {
		t.Fatal(err)
	}
	if iter.Next() {
		t.Fatal("multiple outbox records")
	}
	return record
}

func requireNoRawSearchOutboxRecords(t *testing.T, db *wukongDB, channelID string, channelType uint8) {
	t.Helper()
	lower, upper, err := key.NewSearchOutboxChannelRange(channelID, channelType, 1)
	if err != nil {
		t.Fatal(err)
	}
	iter := db.channelDb(channelID, channelType).NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	defer iter.Close()
	if iter.First() {
		t.Fatalf("unexpected outbox key %x", iter.Key())
	}
}

func requireNoRawSearchOutboxRecordsOnAnyShard(t *testing.T, db *wukongDB, channelID string, channelType uint8) {
	t.Helper()
	lower, upper, err := key.NewSearchOutboxChannelRange(channelID, channelType, 1)
	if err != nil {
		t.Fatal(err)
	}
	for shard := uint32(0); shard < db.shardNum; shard++ {
		iter := db.shardDBById(shard).NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
		found := iter.First()
		var foundKey []byte
		if found {
			foundKey = append([]byte(nil), iter.Key()...)
		}
		if err := iter.Close(); err != nil {
			t.Fatal(err)
		}
		if found {
			t.Fatalf("unexpected outbox key on shard %d: %x", shard, foundKey)
		}
	}
}

func requireSearchOutboxChannelStateEmpty(t *testing.T, db *wukongDB, channelID string, channelType uint8, sequence uint64) {
	t.Helper()
	requireMessageMissing(t, db, channelID, channelType, sequence)
	requireNoRawSearchOutboxRecordsOnAnyShard(t, db, channelID, channelType)
	floor, enabled, err := db.GetSearchOutboxFloor(channelID, channelType)
	if err != nil {
		t.Fatal(err)
	}
	if enabled || floor != 0 {
		t.Fatalf("search outbox floor = %d/%v, want 0/false", floor, enabled)
	}
	lastSequence, _, err := db.GetChannelLastMessageSeq(channelID, channelType)
	if err != nil {
		t.Fatal(err)
	}
	if lastSequence != 0 {
		t.Fatalf("last sequence = %d, want 0", lastSequence)
	}
}

func requireMessageMissing(t *testing.T, db *wukongDB, channelID string, channelType uint8, sequence uint64) {
	t.Helper()
	if _, err := db.LoadMsg(channelID, channelType, sequence); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LoadMsg error = %v, want ErrNotFound", err)
	}
}
