package manager

import (
	"encoding/json"
	"sync"
	"testing"
	"time"
)

func TestTagManagerConcurrentGetAndExpiryReaders(t *testing.T) {
	manager := newTagUpdateTestManager(t)
	tag, err := manager.MakeTagWithTagKey("concurrent-access", []string{"u1"})
	if err != nil {
		t.Fatal(err)
	}
	bucket := manager.getBlucketByTagKey(tag.Key)
	bucket.expire = time.Hour
	const readers = 8
	const iterations = 100
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range readers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range iterations {
				if manager.Get("concurrent-access") != tag {
					t.Error("active tag was lost")
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		for range iterations {
			// 清理、日志和 JSON 输出会与消息取标签同时读取访问时间。
			bucket.checkExpireTags()
			_ = tag.String()
			if _, err := json.Marshal(tag); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	close(start)
	workers.Wait()
	if got := tag.GetCount.Load(); got != readers*iterations {
		t.Fatalf("get count = %d, want %d", got, readers*iterations)
	}
}

func TestTagManagerAccessTimeRules(t *testing.T) {
	t.Run("active read refreshes expiry", func(t *testing.T) {
		manager := newTagUpdateTestManager(t)
		tag, err := manager.MakeTagWithTagKey("active", []string{"u1"})
		if err != nil {
			t.Fatal(err)
		}
		if tag.LastGetTime.Load() != tag.CreatedAt {
			t.Fatal("initial access time differs from creation time")
		}
		bucket := manager.getBlucketByTagKey(tag.Key)
		bucket.expire = time.Hour
		tag.LastGetTime.Store(time.Now().Add(-2 * time.Hour))
		before := time.Now()
		if manager.Get(tag.Key) != tag || tag.LastGetTime.Load().Before(before) {
			t.Fatal("active read did not refresh access time")
		}
		bucket.checkExpireTags()
		if !manager.Exist(tag.Key) {
			t.Fatal("recently accessed tag expired")
		}
		// 无新访问时仍按原有空闲时长清理。
		tag.LastGetTime.Store(time.Now().Add(-2 * time.Hour))
		bucket.checkExpireTags()
		if manager.Get(tag.Key) != nil {
			t.Fatal("idle tag did not expire")
		}
	})
	t.Run("stale node version does not refresh", func(t *testing.T) {
		manager := newTagUpdateTestManager(t)
		tag, err := manager.MakeTagWithTagKey("stale", []string{"u1"})
		if err != nil {
			t.Fatal(err)
		}
		before := tag.LastGetTime.Load()
		manager.nodeVersion = func() uint64 { return 2 }
		if manager.Get(tag.Key) != nil {
			t.Fatal("stale tag was returned")
		}
		if tag.LastGetTime.Load() != before || tag.GetCount.Load() != 0 {
			t.Fatal("stale read changed access time or count")
		}
		if manager.Get("missing") != nil {
			t.Fatal("missing tag was returned")
		}
	})
	t.Run("rename refreshes access time", func(t *testing.T) {
		manager := newTagUpdateTestManager(t)
		tag, err := manager.MakeTagWithTagKey("before", []string{"u1"})
		if err != nil {
			t.Fatal(err)
		}
		tag.LastGetTime.Store(time.Now().Add(-2 * time.Hour))
		before := time.Now()
		if err := manager.RenameTag("before", "after"); err != nil {
			t.Fatal(err)
		}
		if tag.LastGetTime.Load().Before(before) || manager.Exist("before") || !manager.Exist("after") {
			t.Fatal("rename did not preserve tag mapping and access time rules")
		}
	})
}
