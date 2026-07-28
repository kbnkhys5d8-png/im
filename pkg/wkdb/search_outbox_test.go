package wkdb

import (
	"bytes"
	"errors"
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
	t.Helper()
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
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

func requireMessageMissing(t *testing.T, db *wukongDB, channelID string, channelType uint8, sequence uint64) {
	t.Helper()
	if _, err := db.LoadMsg(channelID, channelType, sequence); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LoadMsg error = %v, want ErrNotFound", err)
	}
}
