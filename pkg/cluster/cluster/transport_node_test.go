package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

func TestTermResponseUsesHighPriorityTransport(t *testing.T) {
	if !isHighPriority(types.Event{Type: types.TermResp}) {
		t.Fatal("TermResp must use the high-priority transport queue")
	}
}
