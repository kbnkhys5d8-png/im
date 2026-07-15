package event

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
)

func TestUserHandlerProcessingReservationIsExclusive(t *testing.T) {
	h := &userHandler{}
	if !h.tryBeginProcessing() {
		t.Fatal("expected first processing reservation to succeed")
	}
	if h.tryBeginProcessing() {
		t.Fatal("expected second processing reservation to be rejected")
	}
	h.finishProcessing()
	if !h.tryBeginProcessing() {
		t.Fatal("expected reservation to succeed after processing finished")
	}
}

func TestUserPollerRunsTaskInlineWhenWorkerPoolIsFull(t *testing.T) {
	oldOptions := options.G
	if options.G == nil {
		options.G = options.New()
	}
	oldSize := options.G.Poller.UserGoroutine
	options.G.Poller.UserGoroutine = 1
	t.Cleanup(func() {
		options.G.Poller.UserGoroutine = oldSize
		options.G = oldOptions
	})

	poller := newPoller(0, &EventPool{})
	t.Cleanup(poller.handlePool.Release)
	started := make(chan struct{})
	release := make(chan struct{})
	if err := poller.handlePool.Submit(func() {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("occupy worker: %v", err)
	}
	<-started

	var ran atomic.Bool
	done := make(chan struct{})
	go func() {
		poller.submitTask(func() { ran.Store(true) })
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("task blocked instead of running inline")
	}
	close(release)
	if !ran.Load() {
		t.Fatal("fallback task was dropped")
	}
}

func TestUserHandlerTimeoutRequiresIdleQueue(t *testing.T) {
	oldOptions := options.G
	options.G = options.New()
	options.G.Poller.UserTimeout = time.Nanosecond
	t.Cleanup(func() { options.G = oldOptions })

	h := &userHandler{}
	h.pending.eventQueue = eventbus.NewEventQueue("user-timeout-test")
	h.lastActive.Store(0)
	if !h.isTimeout() {
		t.Fatal("expected an old idle handler to time out")
	}
	h.processing.Store(true)
	if h.isTimeout() {
		t.Fatal("processing handler must not time out")
	}
	h.processing.Store(false)
	h.addEvent(&eventbus.Event{})
	if h.isTimeout() {
		t.Fatal("handler with queued events must not time out")
	}
}
