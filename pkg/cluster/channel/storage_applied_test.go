package channel

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

func TestStorageRaftStateUsesPhysicalTailAndIgnoresSearchWatermark(t *testing.T) {
	db := &appliedWatermarkTestDB{last: wkdb.Message{Term: 4}, appliedErr: errors.New("search metadata unavailable")}
	db.last.MessageSeq = 2
	state, err := newStorage(db, nil).GetState("channel", 2)
	if err != nil {
		t.Fatal(err)
	}
	if state.LastLogIndex != 2 || state.AppliedIndex != 2 || state.LastTerm != 4 {
		t.Fatalf("state = %+v, want physical/applied=2 term=4", state)
	}
	if db.appliedReads != 0 {
		t.Fatalf("Raft state read search watermark %d times", db.appliedReads)
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
	return 0, d.appliedErr
}

func (d *appliedWatermarkTestDB) TruncateLogTo(_ string, _ uint8, index uint64) error {
	d.truncateCalls++
	d.truncatedTo = index
	return nil
}
