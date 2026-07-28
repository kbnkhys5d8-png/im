package plugin

import (
	"errors"
	"sync"
)

var errSearchOutboxUnavailable = errors.New("search outbox is unavailable")

type searchOutboxReadiness struct {
	mu           sync.RWMutex
	runtimeReady func() error
}

func (r *searchOutboxReadiness) setRuntimeReady(check func() error) {
	r.mu.Lock()
	r.runtimeReady = check
	r.mu.Unlock()
}

func (r *searchOutboxReadiness) check() error {
	r.mu.RLock()
	runtimeReady := r.runtimeReady
	r.mu.RUnlock()
	if runtimeReady == nil || runtimeReady() != nil {
		return errSearchOutboxUnavailable
	}
	return nil
}
