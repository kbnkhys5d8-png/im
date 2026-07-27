package wkdb

import "testing"

func TestMessageResultCapacity(t *testing.T) {
	tests := []struct {
		name   string
		limit  int
		minSeq uint64
		maxSeq uint64
		want   int
	}{
		{
			name:   "negative limit",
			limit:  -1,
			minSeq: 1,
			maxSeq: 101,
			want:   0,
		},
		{
			name:   "unlimited",
			limit:  0,
			minSeq: 1,
			maxSeq: 101,
			want:   0,
		},
		{
			name:   "empty range",
			limit:  100,
			minSeq: 101,
			maxSeq: 101,
			want:   0,
		},
		{
			name:   "reversed range",
			limit:  100,
			minSeq: 102,
			maxSeq: 101,
			want:   0,
		},
		{
			name:   "range smaller than limit",
			limit:  100,
			minSeq: 91,
			maxSeq: 101,
			want:   10,
		},
		{
			name:   "limit smaller than range",
			limit:  100,
			minSeq: 1,
			maxSeq: 1001,
			want:   100,
		},
		{
			name:   "at preallocation cap",
			limit:  128,
			minSeq: 1,
			maxSeq: 129,
			want:   128,
		},
		{
			name:   "above preallocation cap",
			limit:  10000,
			minSeq: 1,
			maxSeq: 20001,
			want:   128,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := messageResultCapacity(
				tt.limit,
				tt.minSeq,
				tt.maxSeq,
			); got != tt.want {
				t.Fatalf(
					"messageResultCapacity(%d, %d, %d) = %d, want %d",
					tt.limit,
					tt.minSeq,
					tt.maxSeq,
					got,
					tt.want,
				)
			}
		})
	}
}
