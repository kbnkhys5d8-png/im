package wkdb

import (
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeviceCacheRejectsStaleTokenFill(t *testing.T) {
	current := Device{Id: 1, Uid: "cache-fenced-user", DeviceFlag: 0, DeviceLevel: 1, Token: "new-token", TokenVersion: 42}
	for _, tc := range []struct {
		name    string
		version uint64
	}{
		{name: "older", version: 41},
		{name: "legacy", version: 0},
		{name: "same_version_conflict", version: 42},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := NewDeviceCache(10)
			cache.SetDevice(current)
			late := current
			late.Token, late.TokenVersion = "late-old-token", tc.version
			cache.SetDevice(late)
			got, found := cache.GetDevice(current.Uid, current.DeviceFlag)
			require.True(t, found)
			require.Equal(t, current.Token, got.Token, "旧快照不得覆盖新 Token")
			require.Equal(t, current.TokenVersion, got.TokenVersion)
		})
	}
}

func TestDeviceCacheLoadRejectsDelayedSnapshot(t *testing.T) {
	for _, name := range []string{"updated", "evicted", "invalidated", "user_invalidated", "id_invalidated", "cleared", "legacy", "read_error", "only_invalidated", "only_user_invalidated", "only_id_invalidated", "only_cleared"} {
		t.Run(name, func(t *testing.T) {
			db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
			require.NoError(t, db.Open())
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			db.deviceCache.cache.Resize(1)
			old := Device{Id: 1, Uid: "delayed-fill-user", DeviceFlag: 0, DeviceLevel: 1, Token: "old-token", TokenVersion: 41}
			if name == "legacy" {
				old.TokenVersion = 0
			}
			require.NoError(t, db.AddDevice(old))
			db.deviceCache.ClearCache()
			loaded, release, exited := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(func() {
				unblock()
				select {
				case <-exited:
				case <-time.After(5 * time.Second):
					t.Error("缓存查询未退出")
				}
			})
			type result struct {
				device Device
				err    error
			}
			results := make(chan result, 1)
			var calls atomic.Int32
			go func() {
				defer close(exited)
				device, err := db.deviceCache.getOrLoadDevice(old.Uid, old.DeviceFlag, func() (Device, error) {
					device, err := db.getDeviceFromDB(old.Uid, old.DeviceFlag)
					if calls.Add(1) == 1 {
						close(loaded)
						<-release
						if name == "read_error" {
							return EmptyDevice, errors.New("旧查询异常")
						}
					}
					return device, err
				})
				results <- result{device: device, err: err}
			}()
			select {
			case <-loaded:
			case <-time.After(5 * time.Second):
				t.Fatal("未进入旧查询回填窗口")
			}

			newer := old
			if !strings.HasPrefix(name, "only_") {
				newer.Token = "new-token"
				if name != "legacy" {
					newer.TokenVersion = 42
				}
				require.NoError(t, db.UpdateDevice(newer))
			}
			action := strings.TrimPrefix(name, "only_")
			switch action {
			case "evicted":
				db.deviceCache.SetDevice(Device{Id: 2, Uid: "other-user", DeviceFlag: 0, Token: "other-token"})
			case "invalidated":
				db.deviceCache.InvalidateDevice(old.Uid, old.DeviceFlag)
			case "user_invalidated":
				db.deviceCache.InvalidateUserDevices(old.Uid)
			case "id_invalidated":
				db.deviceCache.InvalidateByDeviceId(old.Id)
			case "cleared":
				db.deviceCache.ClearCache()
			}

			// 更新或失效后才恢复旧查询，失效时没有缓存项也必须重新读库。
			unblock()
			select {
			case got := <-results:
				require.NoError(t, got.err)
				require.Equal(t, newer.Token, got.device.Token)
				require.Equal(t, newer.TokenVersion, got.device.TokenVersion)
			case <-time.After(5 * time.Second):
				t.Fatal("缓存查询阻塞")
			}
			current, err := db.GetDevice(old.Uid, old.DeviceFlag)
			require.NoError(t, err)
			require.Equal(t, newer.Token, current.Token)
			if action == "evicted" || action == "invalidated" || action == "user_invalidated" || action == "id_invalidated" || action == "cleared" {
				require.GreaterOrEqual(t, calls.Load(), int32(2), "缓存不存在时必须重新读库，不能使用旧快照")
			}
		})
	}
}

func TestDeviceCacheConcurrentVersionUpdates(t *testing.T) {
	cache := NewDeviceCache(10)
	var workers sync.WaitGroup
	for version := uint64(1); version <= 64; version++ {
		workers.Add(1)
		go func(version uint64) {
			defer workers.Done()
			cache.SetDevice(Device{Id: 1, Uid: "concurrent-user", DeviceFlag: 0, Token: "token", TokenVersion: version})
		}(version)
	}
	workers.Wait()
	current, found := cache.GetDevice("concurrent-user", 0)
	require.True(t, found)
	require.Equal(t, uint64(64), current.TokenVersion)
}

func TestDeviceCacheExpirationDoesNotRemoveRefresh(t *testing.T) {
	cache := NewDeviceCache(10)
	for i := 0; i < 100; i++ {
		cache.SetCacheTTL(-time.Nanosecond)
		cache.SetDevice(Device{Id: 1, Uid: "expired-user", DeviceFlag: 0, Token: "old", TokenVersion: uint64(i*2 + 1)})
		_, found := cache.GetDevice("expired-user", 0)
		require.False(t, found)
		cache.SetCacheTTL(time.Hour)
		cache.SetDevice(Device{Id: 1, Uid: "expired-user", DeviceFlag: 0, Token: "new", TokenVersion: uint64(i*2 + 2)})
		runtime.Gosched()
		current, found := cache.GetDevice("expired-user", 0)
		require.True(t, found)
		require.Equal(t, "new", current.Token)
	}
}
