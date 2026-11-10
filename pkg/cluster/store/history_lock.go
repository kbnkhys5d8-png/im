package store

import "sync"

type historyLockKey struct {
	channel string
	kind    uint8
}

type historyChannelLock struct {
	mu     sync.RWMutex
	config sync.Mutex
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
		entry = &historyChannelLock{}
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
	key := s.historyKey(channel, kind)
	entry := s.historyLocks.retain(key)
	entry.mu.RLock()
	return func() {
		entry.mu.RUnlock()
		s.historyLocks.release(key, entry)
	}
}

func (s *Store) historyWriteLock(channel string, kind uint8) func() {
	key := s.historyKey(channel, kind)
	entry := s.historyLocks.retain(key)
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		s.historyLocks.release(key, entry)
	}
}

// 配置可能从已持有频道读锁的 append 回调进入，必须保留独立的按频道互斥。
func (s *Store) historyConfigLock(channel string, kind uint8) func() {
	key := s.historyKey(channel, kind)
	entry := s.historyLocks.retain(key)
	entry.config.Lock()
	return func() {
		entry.config.Unlock()
		s.historyLocks.release(key, entry)
	}
}

// 多频道读取不等待冻结中的来源，只在各频道最终门禁检查期间短暂持锁。
func (s *Store) historyVisible(channel string, kind uint8) (bool, error) {
	key := s.historyKey(channel, kind)
	entry := s.historyLocks.retain(key)
	defer s.historyLocks.release(key, entry)
	if !entry.mu.TryRLock() {
		return false, nil
	}
	defer entry.mu.RUnlock()
	fence, err := s.historyFence(channel, kind)
	return fence == nil && err == nil, err
}
