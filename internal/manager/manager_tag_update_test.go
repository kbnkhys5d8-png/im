package manager

import (
	"reflect"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
)

type tagUpdateTestCluster struct {
	icluster.ICluster
}

func (t *tagUpdateTestCluster) GetSlotId(uid string) uint32 {
	if uid == "u2" {
		return 2
	}
	return 1
}

func (t *tagUpdateTestCluster) SlotLeaderId(slotID uint32) uint64 {
	if slotID == 2 {
		return 1002
	}
	return 1001
}

func (t *tagUpdateTestCluster) SlotLeaderNodeInfo(slotID uint32) *clustertypes.Node {
	return &clustertypes.Node{Id: t.SlotLeaderId(slotID)}
}

func newTagUpdateTestManager(t *testing.T) *TagManager {
	t.Helper()
	oldCluster := service.Cluster
	service.Cluster = &tagUpdateTestCluster{}
	t.Cleanup(func() {
		service.Cluster = oldCluster
	})
	manager := NewTagManager(8, func() uint64 { return 1 })
	manager.retiredTagGrace = 20 * time.Millisecond
	return manager
}

func TestUpdateChannelTagInvalidatesPublishedSnapshot(t *testing.T) {
	manager := newTagUpdateTestManager(t)
	oldTag, err := manager.MakeTagWithTagKey("old-tag", []string{"u1"})
	if err != nil {
		t.Fatalf("make tag: %v", err)
	}
	manager.SetChannelTag("group-1", 2, oldTag.Key)

	if err = manager.UpdateChannelTag("group-1", 2, "", []string{"u2"}, false); err != nil {
		t.Fatalf("invalidate channel tag: %v", err)
	}

	if got := manager.GetChannelTag("group-1", 2); got != "" {
		t.Fatalf("expected channel mapping to be invalidated, got %q", got)
	}
	if manager.Exist(oldTag.Key) {
		t.Fatalf("expected old tag %q to stop being active", oldTag.Key)
	}
	if got := manager.Get(oldTag.Key); got != oldTag {
		t.Fatalf("expected old tag %q to remain readable for in-flight messages", oldTag.Key)
	}
	if got := oldTag.GetUsers(); !reflect.DeepEqual(got, []string{"u1"}) {
		t.Fatalf("published snapshot was mutated: %v", got)
	}
}

func TestUpdateChannelTagInvalidatesLegacyExplicitKey(t *testing.T) {
	manager := newTagUpdateTestManager(t)
	legacyTag, err := manager.MakeTagWithTagKey("legacy-tag", []string{"u1"})
	if err != nil {
		t.Fatalf("make legacy tag: %v", err)
	}

	if err = manager.UpdateChannelTag("group-1", 2, legacyTag.Key, []string{"u2"}, false); err != nil {
		t.Fatalf("invalidate legacy channel tag: %v", err)
	}
	if manager.Exist(legacyTag.Key) {
		t.Fatalf("expected legacy tag %q to stop being active", legacyTag.Key)
	}
	if got := manager.Get(legacyTag.Key); got != legacyTag {
		t.Fatalf("expected legacy tag %q to remain readable for in-flight messages", legacyTag.Key)
	}
}

func TestUpdateChannelTagRetiredSnapshotExpires(t *testing.T) {
	manager := newTagUpdateTestManager(t)
	tag, err := manager.MakeTagWithTagKey("expiring-tag", []string{"u1"})
	if err != nil {
		t.Fatalf("make tag: %v", err)
	}
	manager.SetChannelTag("group-1", 2, tag.Key)

	if err = manager.UpdateChannelTag("group-1", 2, "", nil, true); err != nil {
		t.Fatalf("invalidate channel tag: %v", err)
	}
	if manager.Get(tag.Key) == nil {
		t.Fatal("expected retired tag to be readable before grace period expires")
	}

	manager.cleanupRetiredTags(time.Now().Add(manager.retiredTagGrace + time.Millisecond))
	if manager.Get(tag.Key) != nil {
		t.Fatal("expected retired tag to be removed after grace period")
	}
}

func TestReusingRetiredTagKeyDoesNotReviveOldSnapshot(t *testing.T) {
	manager := newTagUpdateTestManager(t)
	oldTag, err := manager.MakeTagWithTagKey("reused-tag", []string{"u1"})
	if err != nil {
		t.Fatalf("make old tag: %v", err)
	}
	manager.SetChannelTag("group-1", 2, oldTag.Key)
	if err = manager.UpdateChannelTag("group-1", 2, "", nil, true); err != nil {
		t.Fatalf("invalidate channel tag: %v", err)
	}

	newTag, err := manager.MakeTagWithTagKey(oldTag.Key, []string{"u2"})
	if err != nil {
		t.Fatalf("make replacement tag: %v", err)
	}
	if got := manager.Get(oldTag.Key); got != newTag {
		t.Fatalf("expected replacement tag, got %#v", got)
	}

	manager.RemoveTag(oldTag.Key)
	if got := manager.Get(oldTag.Key); got != nil {
		t.Fatalf("expected removed replacement not to reveal retired snapshot: %#v", got)
	}
}

func TestRetireTagKeepsFollowerSnapshotForInFlightMessages(t *testing.T) {
	manager := newTagUpdateTestManager(t)
	tag, err := manager.MakeTagWithTagKey("follower-tag", []string{"u1"})
	if err != nil {
		t.Fatalf("make tag: %v", err)
	}

	manager.RetireTag(tag.Key)
	if manager.Exist(tag.Key) {
		t.Fatal("retired follower tag must not remain active")
	}
	if got := manager.Get(tag.Key); got != tag {
		t.Fatalf("expected retired follower tag to remain readable, got %#v", got)
	}
}

func TestRetiredTagReadDoesNotRefreshActiveExpiryTimestamp(t *testing.T) {
	manager := newTagUpdateTestManager(t)
	tag, err := manager.MakeTagWithTagKey("read-only-retired-tag", []string{"u1"})
	if err != nil {
		t.Fatalf("make tag: %v", err)
	}
	lastGetTime := tag.LastGetTime

	manager.RetireTag(tag.Key)
	time.Sleep(time.Millisecond)
	if got := manager.Get(tag.Key); got != tag {
		t.Fatalf("expected retired tag to remain readable, got %#v", got)
	}
	if !tag.LastGetTime.Equal(lastGetTime) {
		t.Fatalf("retired snapshot read changed LastGetTime: got %v want %v", tag.LastGetTime, lastGetTime)
	}
}

func TestChannelTagLockOrdersRebuildBeforeInvalidation(t *testing.T) {
	manager := newTagUpdateTestManager(t)
	rebuildStarted := make(chan struct{})
	allowRebuildToPublish := make(chan struct{})
	rebuildDone := make(chan error, 1)

	go func() {
		rebuildDone <- manager.WithChannelTagLock("group-1", 2, func() error {
			close(rebuildStarted)
			<-allowRebuildToPublish
			tag, err := manager.MakeTagWithTagKey("rebuilt-tag", []string{"u1"})
			if err != nil {
				return err
			}
			manager.SetChannelTag("group-1", 2, tag.Key)
			return nil
		})
	}()
	<-rebuildStarted

	invalidateDone := make(chan error, 1)
	go func() {
		invalidateDone <- manager.UpdateChannelTag("group-1", 2, "", []string{"u2"}, false)
	}()

	select {
	case err := <-invalidateDone:
		t.Fatalf("invalidation overtook an in-flight rebuild: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(allowRebuildToPublish)
	if err := <-rebuildDone; err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if err := <-invalidateDone; err != nil {
		t.Fatalf("invalidate: %v", err)
	}
	if got := manager.GetChannelTag("group-1", 2); got != "" {
		t.Fatalf("expected rebuild to be invalidated, got mapping %q", got)
	}
}
