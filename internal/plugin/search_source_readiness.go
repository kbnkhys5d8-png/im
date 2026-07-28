package plugin

import (
	"errors"
	"sync"
)

var errSearchSourceUnavailable = errors.New("search source is unavailable")

type searchSourceReadiness struct {
	mu           sync.RWMutex
	runtimeReady func() error
}

func (r *searchSourceReadiness) setRuntimeReady(check func() error) {
	r.mu.Lock()
	r.runtimeReady = check
	r.mu.Unlock()
}

func (r *searchSourceReadiness) check() error {
	r.mu.RLock()
	runtimeReady := r.runtimeReady
	r.mu.RUnlock()
	if runtimeReady == nil || runtimeReady() != nil {
		return errSearchSourceUnavailable
	}
	return nil
}
