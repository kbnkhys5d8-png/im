package store

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

func TestHistoryCancelledRequestStopsWaitingForChannel(t *testing.T) {
	for _, mode := range []string{"freeze", "release", "export", "append", "config"} {
		t.Run(mode, func(t *testing.T) {
			db := historyTestDB(t, t.TempDir())
			defer db.Close()
			s := historyTestStore(db, &historyTestChannel{db: db})
			hold := s.historyWriteLock
			if mode == "config" {
				hold = s.historyConfigLock
			}
			unlock := hold("a@c", 1)
			s.historyLocks.mu.Lock()
			entry := s.historyLocks.entries[s.historyKey("a@c", 1)]
			gate := entry.mu
			if mode == "config" {
				gate = entry.config
			}
			s.historyLocks.mu.Unlock()
			var once sync.Once
			release := func() { once.Do(unlock) }
			defer release()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				var err error
				switch mode {
				case "release":
					err = s.ReleaseHistory(ctx, historyIdentity())
				case "export":
					_, err = s.ExportHistory(ctx, historyIdentity(), 0, 10)
				case "append":
					_, err = s.AppendMessages(ctx, "a@c", 1, nil)
				default:
					_, err = s.FreezeHistory(ctx, historyIdentity())
				}
				done <- err
			}()
			deadline := time.Now().Add(time.Second)
			for {
				// 当前固定版本的信号量仅在等待队列为空时允许零权重准入；引用数不能证明已经排队。
				if !gate.TryAcquire(0) {
					break
				}
				gate.Release(0)
				if time.Now().After(deadline) {
					t.Fatal("请求没有进入等锁状态")
				}
				time.Sleep(time.Millisecond)
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("取消后仍在等待频道锁")
			}
			release()
			s.historyLocks.mu.Lock()
			defer s.historyLocks.mu.Unlock()
			if len(s.historyLocks.entries) != 0 {
				t.Fatal("取消的请求泄漏了锁引用")
			}
		})
	}
}

func TestHistoryCancelBeforeFreezePersistsAcrossRestart(t *testing.T) {
	path := t.TempDir()
	db := historyTestDB(t, path)
	s := historyTestStore(db, &historyTestChannel{db: db})
	identity := historyIdentity()
	historyMust(t, s.ReleaseHistory(context.Background(), identity))
	historyMust(t, db.Close())
	db = historyTestDB(t, path)
	defer db.Close()
	s = historyTestStore(db, &historyTestChannel{db: db})
	if _, err := s.FreezeHistory(context.Background(), identity); !errors.Is(err, wkdb.ErrHistoryConflict) {
		t.Fatal("先取消的任务又创建了来源门禁", err)
	}
	current, err := db.(wkdb.HistoryFenceStore).GetHistoryFence(identity.ChannelID, 1)
	historyMust(t, err)
	if current != nil {
		t.Fatal("取消任务仍有活动门禁")
	}
	next := identity
	next.TaskID = strings.Repeat("b", 64)
	_, err = s.FreezeHistory(context.Background(), next)
	historyMust(t, err)
	identity.HighWatermark = 31
	historyMust(t, db.(wkdb.HistoryFenceStore).SetHistoryFence(identity))
	current, err = db.(wkdb.HistoryFenceStore).GetHistoryFence(identity.ChannelID, 1)
	historyMust(t, err)
	if current == nil || current.TaskID != next.TaskID {
		t.Fatal("迟到旧日志改变了新任务的门禁")
	}
}
