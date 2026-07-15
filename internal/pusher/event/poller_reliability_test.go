package event

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
)

func TestPushHandlerProcessingReservationIsExclusive(t *testing.T) {
	h := &pushHandler{}
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

func TestPushPollerUsesPushWorkerLimit(t *testing.T) {
	oldOptions := options.G
	if options.G == nil {
		options.G = options.New()
	}
	oldChannelSize := options.G.Poller.ChannelGoroutine
	oldPushSize := options.G.Poller.PushGoroutine
	options.G.Poller.ChannelGoroutine = 7
	options.G.Poller.PushGoroutine = 1
	t.Cleanup(func() {
		options.G.Poller.ChannelGoroutine = oldChannelSize
		options.G.Poller.PushGoroutine = oldPushSize
		options.G = oldOptions
	})

	poller := newPoller(0, &EventPool{})
	t.Cleanup(poller.handlePool.Release)
	if got := poller.handlePool.Cap(); got != 1 {
		t.Fatalf("expected push worker capacity 1, got %d", got)
	}
}

func TestPushHandlerUsesPushBatchLimit(t *testing.T) {
	oldOptions := options.G
	if options.G == nil {
		options.G = options.New()
	}
	oldUserBatch := options.G.Poller.UserEventMaxSizePerBatch
	oldPushBatch := options.G.Poller.PushEventMaxSizePerBatch
	options.G.Poller.UserEventMaxSizePerBatch = 10
	options.G.Poller.PushEventMaxSizePerBatch = 1
	t.Cleanup(func() {
		options.G.Poller.UserEventMaxSizePerBatch = oldUserBatch
		options.G.Poller.PushEventMaxSizePerBatch = oldPushBatch
		options.G = oldOptions
	})

	h := &pushHandler{}
	h.pending.eventQueue = eventbus.NewEventQueue("push-test")
	h.addEvent(&eventbus.Event{})
	h.addEvent(&eventbus.Event{})
	if got := len(h.events()); got != 1 {
		t.Fatalf("expected one event from push batch, got %d", got)
	}
}

func TestPushPollerRunsTaskInlineWhenWorkerPoolIsFull(t *testing.T) {
	oldOptions := options.G
	if options.G == nil {
		options.G = options.New()
	}
	oldSize := options.G.Poller.PushGoroutine
	options.G.Poller.PushGoroutine = 1
	t.Cleanup(func() {
		options.G.Poller.PushGoroutine = oldSize
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
