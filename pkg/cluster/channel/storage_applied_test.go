package channel

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestStorageGetLogsPreservesSearchOutbox(t *testing.T) {
	message := wkdb.Message{
		RecvPacket: wkproto.RecvPacket{
			MessageID: 12, MessageSeq: 1, ClientMsgNo: "catch-up",
			ChannelID: "channel", ChannelType: 2, Payload: []byte("body"),
		},
		Term: 6, SearchOutbox: true,
	}
	db := &appliedWatermarkTestDB{
		last:     message,
		messages: []wkdb.Message{message},
	}
	logs, err := newStorage(db, nil).GetLogs(
		wkutil.ChannelToKey("channel", 2), 1, 2, 1024,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("logs length = %d, want 1", len(logs))
	}
	var decoded wkdb.Message
	if err := decoded.Unmarshal(logs[0].Data); err != nil {
		t.Fatal(err)
	}
	if !decoded.SearchOutbox {
		t.Fatal("follower catch-up log lost SearchOutbox")
	}
}

func TestStorageRaftStateUsesFloorAndDurableAppliedForOutboxChannel(t *testing.T) {
	db := &appliedWatermarkTestDB{
		last:          wkdb.Message{Term: 4},
		applied:       41,
		outboxFloor:   40,
		outboxEnabled: true,
	}
	db.last.MessageSeq = 42
	state, err := newStorage(db, nil).GetState("channel", 2)
	if err != nil {
		t.Fatal(err)
	}
	if state.LastLogIndex != 42 || state.AppliedIndex != 41 || state.LastTerm != 4 {
		t.Fatalf("state = %+v, want last=42 applied=41 term=4", state)
	}
}

func TestStorageRaftStateUsesFloorWhenDurableAppliedIsOlder(t *testing.T) {
	db := &appliedWatermarkTestDB{
		last:          wkdb.Message{Term: 4},
		applied:       0,
		outboxFloor:   40,
		outboxEnabled: true,
	}
	db.last.MessageSeq = 42
	state, err := newStorage(db, nil).GetState("channel", 2)
	if err != nil || state.AppliedIndex != 40 {
		t.Fatalf("state = %+v, err=%v, want applied=40", state, err)
	}
}

func TestStorageRaftStateKeepsPhysicalTailForLegacyChannel(t *testing.T) {
	db := &appliedWatermarkTestDB{
		last:          wkdb.Message{Term: 4},
		applied:       0,
		outboxEnabled: false,
	}
	db.last.MessageSeq = 42
	state, err := newStorage(db, nil).GetState("legacy", 2)
	if err != nil || state.AppliedIndex != 42 {
		t.Fatalf("state = %+v, err=%v, want applied=42", state, err)
	}
	if db.appliedReads != 0 {
		t.Fatalf("legacy restart read durable applied %d times", db.appliedReads)
	}
}

func TestStorageRaftStateRejectsOutboxFloorOrAppliedBeyondPhysicalTail(t *testing.T) {
	for _, fixture := range []*appliedWatermarkTestDB{
		{last: wkdb.Message{}, outboxFloor: 2, outboxEnabled: true},
		{last: wkdb.Message{}, applied: 2, outboxFloor: 0, outboxEnabled: true},
	} {
		fixture.last.MessageSeq = 1
		if _, err := newStorage(fixture, nil).GetState("channel", 2); err == nil {
			t.Fatal("outbox restart watermark beyond physical tail was accepted")
		}
	}
}

func TestStorageApplyAndTruncateDoNotDependOnSearchWatermark(t *testing.T) {
	db := &appliedWatermarkTestDB{appliedErr: errors.New("search metadata unavailable")}
	storage := newStorage(db, nil)
	key := wkutil.ChannelToKey("channel", 2)
	if err := storage.Apply(key, []types.Log{{Index: 2}}); err != nil {
		t.Fatalf("no-op core apply returned search error: %v", err)
	}
	if err := storage.TruncateLogTo(key, 1); err != nil {
		t.Fatalf("truncate depended on search watermark: %v", err)
	}
	if db.appliedReads != 0 {
		t.Fatalf("core storage read search watermark %d times", db.appliedReads)
	}
	if db.truncateCalls != 1 || db.truncatedTo != 1 {
		t.Fatalf("truncate calls=%d index=%d, want 1/1", db.truncateCalls, db.truncatedTo)
	}
}

type appliedWatermarkTestDB struct {
	wkdb.DB
	last          wkdb.Message
	messages      []wkdb.Message
	applied       uint64
	outboxFloor   uint64
	outboxEnabled bool
	appliedErr    error
	appliedReads  int
	truncateCalls int
	truncatedTo   uint64
}

func (d *appliedWatermarkTestDB) GetLastMsg(string, uint8) (wkdb.Message, error) {
	return d.last, nil
}

func (d *appliedWatermarkTestDB) GetChannelAppliedIndex(string, uint8) (uint64, error) {
	d.appliedReads++
	return d.applied, d.appliedErr
}

func (d *appliedWatermarkTestDB) GetSearchOutboxFloor(string, uint8) (uint64, bool, error) {
	return d.outboxFloor, d.outboxEnabled, nil
}

func (d *appliedWatermarkTestDB) LoadNextRangeMsgsForSize(
	string, uint8, uint64, uint64, uint64,
) ([]wkdb.Message, error) {
	return d.messages, nil
}

func (d *appliedWatermarkTestDB) GetChannelLastMessageSeq(
	string, uint8,
) (uint64, uint64, error) {
	return uint64(d.last.MessageSeq), 0, nil
}

func (d *appliedWatermarkTestDB) TruncateLogTo(_ string, _ uint8, index uint64) error {
	d.truncateCalls++
	d.truncatedTo = index
	return nil
}
