package api

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func waitForChannelTagRetry(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for channel tag retry condition")
}

func TestChannelTagRetryQueueRetriesPastThreeFailures(t *testing.T) {
	var calls atomic.Int32
	queue := newChannelTagRetryQueue(1, func(channelTagInvalidationTarget) error {
		if calls.Add(1) < 5 {
			return errors.New("temporary cluster failure")
		}
		return nil
	})
	queue.backoff = func(int) time.Duration { return time.Millisecond }
	queue.Start()
	t.Cleanup(queue.Stop)

	queue.Enqueue(channelTagInvalidationTarget{channelID: "group-1", channelType: 2})
	waitForChannelTagRetry(t, time.Second, func() bool {
		return calls.Load() >= 5 && queue.Pending() == 0
	})
}

func TestChannelTagRetryQueueRecoversFromInvalidationPanic(t *testing.T) {
	var calls atomic.Int32
	queue := newChannelTagRetryQueue(1, func(channelTagInvalidationTarget) error {
		if calls.Add(1) == 1 {
			panic("temporary invalidation panic")
		}
		return nil
	})
	queue.backoff = func(int) time.Duration { return time.Millisecond }
	queue.Start()
	t.Cleanup(queue.Stop)

	queue.Enqueue(channelTagInvalidationTarget{channelID: "group-panic", channelType: 2})
	waitForChannelTagRetry(t, time.Second, func() bool {
		return calls.Load() == 2 && queue.Pending() == 0
	})
}

func TestChannelTagRetryQueueRequeuesChangedGeneration(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	queue := newChannelTagRetryQueue(1, func(channelTagInvalidationTarget) error {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-releaseFirst
		}
		return nil
	})
	queue.Start()
	t.Cleanup(queue.Stop)

	target := channelTagInvalidationTarget{channelID: "group-1", channelType: 2}
	queue.Enqueue(target)
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first invalidation did not start")
	}
	queue.Enqueue(target)
	close(releaseFirst)

	waitForChannelTagRetry(t, time.Second, func() bool {
		return calls.Load() == 2 && queue.Pending() == 0
	})
}

func TestChannelTagRetryQueueUsesFixedWorkers(t *testing.T) {
	const workers = 2
	release := make(chan struct{})
	var current atomic.Int32
	var maximum atomic.Int32
	queue := newChannelTagRetryQueue(workers, func(channelTagInvalidationTarget) error {
		now := current.Add(1)
		for {
			old := maximum.Load()
			if now <= old || maximum.CompareAndSwap(old, now) {
				break
			}
		}
		<-release
		current.Add(-1)
		return nil
	})
	queue.Start()

	for i := 0; i < 20; i++ {
		queue.Enqueue(channelTagInvalidationTarget{channelID: string(rune('a' + i)), channelType: 2})
	}
	waitForChannelTagRetry(t, time.Second, func() bool { return maximum.Load() == workers })
	if got := maximum.Load(); got > workers {
		t.Fatalf("expected at most %d concurrent workers, got %d", workers, got)
	}
	close(release)
	waitForChannelTagRetry(t, time.Second, func() bool { return queue.Pending() == 0 })
	queue.Stop()
}
