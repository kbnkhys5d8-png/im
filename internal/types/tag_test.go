package types

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTagAccessTimeJSONCompatibility(t *testing.T) {
	tests := []struct {
		name string
		at   time.Time
	}{
		{name: "zero"},
		{name: "utc", at: time.Date(2026, 9, 28, 1, 2, 3, 123456789, time.UTC)},
		{name: "offset", at: time.Date(2026, 9, 28, 9, 2, 3, 0, time.FixedZone("CST", 8*3600))},
		{name: "monotonic", at: time.Now()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag := &Tag{Key: "tag"}
			tag.LastGetTime.Store(tt.at)
			tag.GetCount.Store(7)
			if tag.LastGetTime.Load() != tt.at {
				t.Fatal("stored time lost its original clock information")
			}
			encoded, err := json.Marshal(tag)
			if err != nil {
				t.Fatal(err)
			}
			fields := make(map[string]json.RawMessage)
			if err := json.Unmarshal(encoded, &fields); err != nil {
				t.Fatal(err)
			}
			want, err := tt.at.MarshalJSON()
			if err != nil {
				t.Fatal(err)
			}
			if got := string(fields["last_get_time"]); got != string(want) {
				t.Fatalf("last_get_time = %s, want %s", got, want)
			}
			var decoded Tag
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if !decoded.LastGetTime.Load().Equal(tt.at) || decoded.GetCount.Load() != 7 {
				t.Fatal("tag JSON round trip changed access time or count")
			}
		})
	}
}

func TestTagAccessTimeJSONNullAndInvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "null", input: "null"},
		{name: "invalid string", input: `"invalid"`},
		{name: "number", input: "123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tag := &Tag{}
			reference := time.Now()
			tag.LastGetTime.Store(reference)
			// 包含错误返回后的字段状态，也与原 time.Time 行为保持一致。
			wantErr := reference.UnmarshalJSON([]byte(tt.input))
			gotErr := json.Unmarshal([]byte(`{"last_get_time":`+tt.input+`}`), tag)
			if (gotErr != nil) != (wantErr != nil) || tag.LastGetTime.Load() != reference {
				t.Fatalf("time JSON behavior differs: got %v (%v), want %v (%v)",
					tag.LastGetTime.Load(), gotErr, reference, wantErr)
			}
		})
	}
}
