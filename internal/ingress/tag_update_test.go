package ingress

import (
	"errors"
	"reflect"
	"testing"
)

type fakeChannelTagManager struct {
	channelID   string
	channelType uint8
	tagKey      string
	uids        []string
	remove      bool
	err         error
}

func (f *fakeChannelTagManager) UpdateChannelTag(channelID string, channelType uint8, tagKey string, uids []string, remove bool) error {
	f.channelID = channelID
	f.channelType = channelType
	f.tagKey = tagKey
	f.uids = append([]string(nil), uids...)
	f.remove = remove
	return f.err
}

func TestUpdateChannelTagUsesCurrentMapping(t *testing.T) {
	manager := &fakeChannelTagManager{}
	err := UpdateChannelTag(manager, "group-1", 2, []string{"u1", "u2"}, false)
	if err != nil {
		t.Fatalf("update channel tag: %v", err)
	}
	if manager.channelID != "group-1" || manager.channelType != 2 {
		t.Fatalf("unexpected channel: %q/%d", manager.channelID, manager.channelType)
	}
	if manager.tagKey != "" {
		t.Fatalf("expected no explicit tag key, got %q", manager.tagKey)
	}
	if !reflect.DeepEqual(manager.uids, []string{"u1", "u2"}) || manager.remove {
		t.Fatalf("unexpected update: uids=%v remove=%v", manager.uids, manager.remove)
	}
}

func TestUpdateChannelTagWithKeyPreservesLegacyTagKey(t *testing.T) {
	manager := &fakeChannelTagManager{}
	err := UpdateChannelTagWithKey(manager, "group-1", 2, "legacy-tag", []string{"u1"}, true)
	if err != nil {
		t.Fatalf("update channel tag: %v", err)
	}
	if manager.tagKey != "legacy-tag" {
		t.Fatalf("expected legacy tag key, got %q", manager.tagKey)
	}
	if !manager.remove {
		t.Fatal("expected remove update")
	}
}

func TestUpdateChannelTagReturnsManagerError(t *testing.T) {
	wantErr := errors.New("publish failed")
	manager := &fakeChannelTagManager{err: wantErr}

	err := UpdateChannelTag(manager, "group-1", 2, []string{"u1"}, false)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected %v, got %v", wantErr, err)
	}
}

func TestValidateTagUpdateReqAllowsEmptyUIDsForChannelInvalidation(t *testing.T) {
	err := validateTagUpdateReq(&TagUpdateReq{
		ChannelId:   "group-1",
		ChannelType: 2,
		ChannelTag:  true,
	})
	if err != nil {
		t.Fatalf("expected channel invalidation to allow empty uids, got %v", err)
	}
}

func TestValidateTagUpdateReqRejectsEmptyUIDsForIncrementalTag(t *testing.T) {
	err := validateTagUpdateReq(&TagUpdateReq{TagKey: "tag-1"})
	if err == nil {
		t.Fatal("expected incremental tag update to reject empty uids")
	}
}
