package wkdb

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

func TestUpdateChannelAppliedIndexNeverRegresses(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1)))
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	if err := db.UpdateChannelAppliedIndex("channel", 2, 2); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateChannelAppliedIndex("channel", 2, 1); err != nil {
		t.Fatal(err)
	}
	applied, err := db.GetChannelAppliedIndex("channel", 2)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 2 {
		t.Fatalf("applied index regressed to %d, want 2", applied)
	}
}

func TestGetChannelAppliedIndexRejectsMalformedValue(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
	}{
		{name: "short", raw: []byte{0, 0, 0, 0, 0, 0, 1}},
		{name: "long", raw: []byte{0, 0, 0, 0, 0, 0, 0, 1, 0}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openSearchOutboxTestDB(t)
			setRawChannelAppliedIndex(t, db, test.raw)
			if _, err := getChannelAppliedIndexWithoutPanic(t, db); err == nil {
				t.Fatal("GetChannelAppliedIndex accepted malformed value")
			}
		})
	}
}

func setRawChannelAppliedIndex(t *testing.T, db *wukongDB, raw []byte) {
	t.Helper()
	err := db.channelDb("channel", 2).Set(
		key.NewChannelCommonColumnKey(
			"channel",
			2,
			key.TableChannelCommon.Column.AppliedIndex,
		),
		raw,
		pebble.Sync,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func getChannelAppliedIndexWithoutPanic(
	t *testing.T,
	db *wukongDB,
) (applied uint64, err error) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("GetChannelAppliedIndex panicked on malformed value: %v", recovered)
		}
	}()
	return db.GetChannelAppliedIndex("channel", 2)
}
