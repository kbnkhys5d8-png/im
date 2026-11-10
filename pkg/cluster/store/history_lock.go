package store

import (
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

const historyReaderCapacity int64 = 1 << 30

type historyLockKey struct {
	channel string
	kind    uint8
}

type historyChannelLock struct {
	mu     *semaphore.Weighted
	config *semaphore.Weighted
	refs   int
}

// 全局互斥仅保护引用表；等待频道锁、Drain、提案和读取均不持有它。
type historyLockTable struct {
	mu      sync.Mutex
	entries map[historyLockKey]*historyChannelLock
}

func (t *historyLockTable) retain(key historyLockKey) *historyChannelLock {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.entries == nil {
		t.entries = make(map[historyLockKey]*historyChannelLock)
	}
	entry := t.entries[key]
	if entry == nil {
		entry = &historyChannelLock{mu: semaphore.NewWeighted(historyReaderCapacity), config: semaphore.NewWeighted(1)}
		t.entries[key] = entry
	}
	// 先计入等待者，不能在持有者释放后为同一频道创建第二把锁。
	entry.refs++
	return entry
}

func (t *historyLockTable) release(key historyLockKey, entry *historyChannelLock) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry.refs--
	if entry.refs == 0 {
		delete(t.entries, key)
	}
}

func (s *Store) historyKey(channel string, kind uint8) historyLockKey {
	if kind == 1 && s.opts.HistorySourceChannel != nil {
		channel = s.opts.HistorySourceChannel(channel)
	}
	return historyLockKey{channel: channel, kind: kind}
}

func (s *Store) historyReadLock(channel string, kind uint8) func() {
	unlock, _ := s.historyReadLockContext(context.Background(), channel, kind)
	return unlock
}

func (s *Store) historyWriteLock(channel string, kind uint8) func() {
	unlock, _ := s.historyWriteLockContext(context.Background(), channel, kind)
	return unlock
}

// 配置可能从已持有频道读锁的 append 回调进入，必须保留独立的按频道互斥。
func (s *Store) historyConfigLock(channel string, kind uint8) func() {
	unlock, _ := s.historyConfigLockContext(context.Background(), channel, kind)
	return unlock
}

func (s *Store) historyReadLockContext(ctx context.Context, channel string, kind uint8) (func(), error) {
	return s.historyAcquire(ctx, channel, kind, 1, false)
}

func (s *Store) historyWriteLockContext(ctx context.Context, channel string, kind uint8) (func(), error) {
	return s.historyAcquire(ctx, channel, kind, historyReaderCapacity, false)
}

func (s *Store) historyConfigLockContext(ctx context.Context, channel string, kind uint8) (func(), error) {
	return s.historyAcquire(ctx, channel, kind, 1, true)
}

// 等锁期间也响应请求取消，且取消的等待者立即归还引用，不能遗留后台等待协程。
func (s *Store) historyAcquire(ctx context.Context, channel string, kind uint8, weight int64, config bool) (func(), error) {
	key := s.historyKey(channel, kind)
	entry := s.historyLocks.retain(key)
	gate := entry.mu
	if config {
		gate = entry.config
	}
	if err := gate.Acquire(ctx, weight); err != nil {
		s.historyLocks.release(key, entry)
		return nil, err
	}
	return func() {
		gate.Release(weight)
		s.historyLocks.release(key, entry)
	}, nil
}

// 多频道读取不等待冻结中的来源，只在各频道最终门禁检查期间短暂持锁。
func (s *Store) historyVisible(channel string, kind uint8) (bool, error) {
	key := s.historyKey(channel, kind)
	entry := s.historyLocks.retain(key)
	defer s.historyLocks.release(key, entry)
	if !entry.mu.TryAcquire(1) {
		return false, nil
	}
	defer entry.mu.Release(1)
	fence, err := s.historyFence(channel, kind)
	return fence == nil && err == nil, err
}
