package plugin

import (
	"errors"
	"sync"
)

var errSearchSourceUnavailable = errors.New("search source is unavailable")

type searchSourceReadiness struct {
	mu           sync.RWMutex
	ready        bool
	runtimeReady func() error
}

func (r *searchSourceReadiness) setRuntimeReady(check func() error) {
	r.mu.Lock()
	r.runtimeReady = check
	r.mu.Unlock()
}

func (r *searchSourceReadiness) setBootstrapResult(err error) {
	r.mu.Lock()
	r.ready = err == nil
	r.mu.Unlock()
}

func (r *searchSourceReadiness) check() error {
	r.mu.RLock()
	ready := r.ready
	runtimeReady := r.runtimeReady
	r.mu.RUnlock()
	if !ready {
		return errSearchSourceUnavailable
	}
	if runtimeReady == nil || runtimeReady() != nil {
		return errSearchSourceUnavailable
	}
	return nil
}
