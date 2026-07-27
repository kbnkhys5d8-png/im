package service

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	clusterstore "github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type testSystemAccountManager struct{}

func (testSystemAccountManager) IsSystemAccount(string) bool        { return false }
func (testSystemAccountManager) AddSystemUids([]string) error       { return nil }
func (testSystemAccountManager) AddSystemUidsToCache([]string)      {}
func (testSystemAccountManager) RemoveSystemUids([]string) error    { return nil }
func (testSystemAccountManager) RemoveSystemUidsFromCache([]string) {}

func TestSpoofedSystemDeviceDoesNotBypassSenderPermission(t *testing.T) {
	previousOptions := options.G
	options.G = options.New()
	t.Cleanup(func() {
		options.G = previousOptions
	})

	database := wkdb.NewWukongDB(wkdb.NewOptions(
		wkdb.WithDir(t.TempDir()),
		wkdb.WithShardNum(1),
	))
	if err := database.Open(); err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close test database: %v", err)
		}
	})

	previousStore := Store
	previousSystemAccountManager := SystemAccountManager
	Store = clusterstore.New(clusterstore.NewOptions(clusterstore.WithDB(database)))
	SystemAccountManager = testSystemAccountManager{}
	t.Cleanup(func() {
		Store = previousStore
		SystemAccountManager = previousSystemAccountManager
	})

	permission := NewPermissionService(nil)
	reasonCode, err := permission.HasPermissionForSender(
		"group-1",
		wkproto.ChannelTypeGroup,
		SenderInfo{
			UID: "attacker",
		},
	)
	if err != nil {
		t.Fatalf("check spoofed sender permission: %v", err)
	}
	if reasonCode != wkproto.ReasonSubscriberNotExist {
		t.Fatalf("spoofed sender reason = %s, want %s", reasonCode, wkproto.ReasonSubscriberNotExist)
	}

	reasonCode, err = permission.HasPermissionForSender(
		"group-1",
		wkproto.ChannelTypeGroup,
		SenderInfo{
			UID:             "internal-service",
			TrustedInternal: true,
		},
	)
	if err != nil {
		t.Fatalf("check trusted sender permission: %v", err)
	}
	if reasonCode != wkproto.ReasonSuccess {
		t.Fatalf("trusted sender reason = %s, want %s", reasonCode, wkproto.ReasonSuccess)
	}
}
