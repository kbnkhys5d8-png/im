package api

import (
	"errors"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
)

type fakeChannelTagOwnerResolver struct {
	nodeID      uint64
	err         error
	channelID   string
	channelType uint8
}

func (f *fakeChannelTagOwnerResolver) SlotLeaderIdOfChannel(channelID string, channelType uint8) (uint64, error) {
	f.channelID = channelID
	f.channelType = channelType
	return f.nodeID, f.err
}

func TestResolveChannelTagOwnerUsesSlotLeader(t *testing.T) {
	resolver := &fakeChannelTagOwnerResolver{nodeID: 1003}

	nodeID, err := resolveChannelTagOwner(resolver, "group-1", 2)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if nodeID != 1003 {
		t.Fatalf("expected node 1003, got %d", nodeID)
	}
	if resolver.channelID != "group-1" || resolver.channelType != 2 {
		t.Fatalf("unexpected lookup: channel=%q type=%d", resolver.channelID, resolver.channelType)
	}
}

func TestResolveChannelTagOwnerReturnsLookupError(t *testing.T) {
	wantErr := errors.New("slot leader unavailable")
	resolver := &fakeChannelTagOwnerResolver{err: wantErr}

	_, err := resolveChannelTagOwner(resolver, "group-1", 2)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected lookup error, got %v", err)
	}
}

func TestChannelTagInvalidationTargetsIncludeActiveCMDChannel(t *testing.T) {
	oldOptions := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = oldOptions })

	targets := channelTagInvalidationTargets("group-1", 2, true)
	want := []channelTagInvalidationTarget{
		{channelID: "group-1", channelType: 2},
		{channelID: options.G.OrginalConvertCmdChannel("group-1"), channelType: 2},
	}
	if !reflect.DeepEqual(targets, want) {
		t.Fatalf("unexpected targets: %#v", targets)
	}
}

func TestChannelTagInvalidationTargetsSkipInactiveCMDChannel(t *testing.T) {
	targets := channelTagInvalidationTargets("group-1", 2, false)
	want := []channelTagInvalidationTarget{{channelID: "group-1", channelType: 2}}
	if !reflect.DeepEqual(targets, want) {
		t.Fatalf("unexpected targets: %#v", targets)
	}
}

func TestFailedChannelTagInvalidationsContinueAfterError(t *testing.T) {
	targets := []channelTagInvalidationTarget{
		{channelID: "group-1", channelType: 2},
		{channelID: "group-1@cmd", channelType: 2},
	}
	var called []string
	failures := failedChannelTagInvalidations(targets, func(target channelTagInvalidationTarget) error {
		called = append(called, target.channelID)
		if target.channelID == "group-1" {
			return errors.New("normal tag unavailable")
		}
		return nil
	})
	if !reflect.DeepEqual(called, []string{"group-1", "group-1@cmd"}) {
		t.Fatalf("expected both targets to be attempted, got %v", called)
	}
	if !reflect.DeepEqual(failures, targets[:1]) {
		t.Fatalf("unexpected failures: %#v", failures)
	}
}
