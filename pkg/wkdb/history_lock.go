package wkdb

import "sync"

type historyFenceLock struct {
	mu   sync.RWMutex
	refs int
}

// 引用表只保护建删键；不同频道不共用跨提交等待的互斥锁。
type historyFenceLocks struct {
	mu      sync.Mutex
	entries map[string]*historyFenceLock
}

func (t *historyFenceLocks) acquire(channel string, write bool) func() {
	t.mu.Lock()
	if t.entries == nil {
		t.entries = make(map[string]*historyFenceLock)
	}
	entry := t.entries[channel]
	if entry == nil {
		entry = &historyFenceLock{}
		t.entries[channel] = entry
	}
	entry.refs++
	t.mu.Unlock()
	if write {
		entry.mu.Lock()
	} else {
		entry.mu.RLock()
	}
	return func() {
		if write {
			entry.mu.Unlock()
		} else {
			entry.mu.RUnlock()
		}
		t.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(t.entries, channel)
		}
		t.mu.Unlock()
	}
}
