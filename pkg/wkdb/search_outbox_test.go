package wkdb

import (
	"bytes"
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
			ChannelID: "channel", ChannelType: 2, FromUID: "sender", Payload: []byte("body"),
		},
		Term: 6, SearchOutbox: searchOutbox,
	}
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
