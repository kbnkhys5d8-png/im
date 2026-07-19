package wkdb

import "testing"

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
