package handler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func receiveDeliveryValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("等待内存投递测试信号超时")
		var zero T
		return zero
	}
}

func requireDeliveryQueueEmpty(t *testing.T, q *deliveryQueue) {
	t.Helper()
	require.Eventually(t, func() bool { return q.pendingCount() == 0 }, 5*time.Second, time.Millisecond)
}

func TestDeliveryQueueDrainsFiveThousandAcrossWorkerWaves(t *testing.T) {
	const attemptTimeout = 40 * time.Millisecond
	q := newDeliveryQueue(context.Background(), 16, attemptTimeout)
	t.Cleanup(q.stop)
	const count = 5000
	completed := make(chan int, count)
	start := time.Now()
	for i := range count {
		key := deliveryKey{channelId: "large-group", channelType: 2, uid: fmt.Sprintf("u-%d", i)}
		require.NoError(t, q.enqueue(key, func(ctx context.Context) error {
			// 总任务至少跨越多轮超时窗口，每一波仍有独立的执行预算。
			timer := time.NewTimer(time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
			completed <- i
			return nil
		}))
	}
	seen := make(map[int]bool, count)
	for range count {
		i := receiveDeliveryValue(t, completed)
		require.False(t, seen[i], "已成功任务不能重复执行")
		seen[i] = true
	}
	requireDeliveryQueueEmpty(t, q)
	require.Greater(t, time.Since(start), attemptTimeout, "全部任务不能只覆盖单个整批超时窗口")
}

func TestDeliveryQueueRetryPreservesFIFOAndLetsHealthyKeyProceed(t *testing.T) {
	q := newDeliveryQueue(context.Background(), 1, time.Second)
	t.Cleanup(q.stop)
	failed := errors.New("暂时恢复失败")
	started := make(chan struct{}, 1)
	finishFirst := make(chan struct{})
	finished := make(chan string, 3)
	var attempts atomic.Int32
	key := deliveryKey{channelId: "group", channelType: 2, uid: "slow"}
	require.NoError(t, q.enqueue(key, func(ctx context.Context) error {
		if attempts.Add(1) == 1 {
			started <- struct{}{}
			select {
			case <-finishFirst:
				return failed
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		finished <- "first"
		return nil
	}))
	receiveDeliveryValue(t, started)
	require.NoError(t, q.enqueue(key, func(context.Context) error {
		finished <- "second"
		return nil
	}))
	require.NoError(t, q.enqueue(deliveryKey{channelId: "group", channelType: 2, uid: "healthy"}, func(context.Context) error {
		finished <- "healthy"
		return nil
	}))
	close(finishFirst)
	require.Equal(t, "healthy", receiveDeliveryValue(t, finished))
	require.Equal(t, "first", receiveDeliveryValue(t, finished))
	require.Equal(t, "second", receiveDeliveryValue(t, finished))
	require.EqualValues(t, 2, attempts.Load())
	requireDeliveryQueueEmpty(t, q)
}

func TestDeliveryQueueBoundsConcurrencyAndSerializesEachKey(t *testing.T) {
	const workers, keys, perKey = 4, 32, 4
	q := newDeliveryQueue(context.Background(), workers, time.Second)
	t.Cleanup(q.stop)
	var active atomic.Int32
	var peak atomic.Int32
	var mu sync.Mutex
	next := make(map[deliveryKey]int)
	started := make(chan struct{}, workers)
	release := make(chan struct{})
	results := make(chan error, keys*perKey)
	for seq := range perKey {
		for uid := range keys {
			key := deliveryKey{channelId: "group", channelType: 2, uid: fmt.Sprint(uid)}
			require.NoError(t, q.enqueue(key, func(ctx context.Context) error {
				n := active.Add(1)
				defer active.Add(-1)
				for old := peak.Load(); n > old && !peak.CompareAndSwap(old, n); old = peak.Load() {
				}
				if seq == 0 && uid < workers {
					started <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				mu.Lock()
				var err error
				if next[key] != seq {
					err = fmt.Errorf("用户 %s 的顺序为 %d，预期 %d", key.uid, seq, next[key])
				}
				next[key]++
				mu.Unlock()
				results <- err
				return nil
			}))
		}
	}
	for range workers {
		receiveDeliveryValue(t, started)
	}
	require.EqualValues(t, workers, peak.Load())
	close(release)
	for range keys * perKey {
		require.NoError(t, receiveDeliveryValue(t, results))
	}
	requireDeliveryQueueEmpty(t, q)
	require.LessOrEqual(t, peak.Load(), int32(workers))
}

func TestDeliveryQueueTimeoutStartsPerAttemptAndRetryGetsFreshDeadline(t *testing.T) {
	q := newDeliveryQueue(context.Background(), 1, 40*time.Millisecond)
	t.Cleanup(q.stop)
	deadlines := make(chan time.Time, 3)
	var attempts atomic.Int32
	require.NoError(t, q.enqueue(deliveryKey{uid: "retry"}, func(ctx context.Context) error {
		deadline, _ := ctx.Deadline()
		deadlines <- deadline
		if attempts.Add(1) == 1 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}))
	first := receiveDeliveryValue(t, deadlines)
	// 此任务排队等前一个超时，开始执行后仍应获得完整的新超时。
	require.NoError(t, q.enqueue(deliveryKey{uid: "queued"}, func(ctx context.Context) error {
		deadline, _ := ctx.Deadline()
		deadlines <- deadline
		return ctx.Err()
	}))
	second := receiveDeliveryValue(t, deadlines)
	third := receiveDeliveryValue(t, deadlines)
	require.True(t, second.After(first), "排队等待不应消耗自己的执行超时")
	require.True(t, third.After(second), "重试不能沿用已经取消的 context")
	requireDeliveryQueueEmpty(t, q)
	require.EqualValues(t, 2, attempts.Load())
}

func TestDeliveryQueueStopCancelsAttemptAndRejectsNewWork(t *testing.T) {
	q := newDeliveryQueue(context.Background(), 1, time.Minute)
	t.Cleanup(q.stop)
	started := make(chan struct{}, 1)
	canceled := make(chan error, 1)
	require.NoError(t, q.enqueue(deliveryKey{uid: "running"}, func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		canceled <- ctx.Err()
		return ctx.Err()
	}))
	receiveDeliveryValue(t, started)
	var waitingExecuted atomic.Bool
	require.NoError(t, q.enqueue(deliveryKey{uid: "waiting"}, func(context.Context) error {
		waitingExecuted.Store(true)
		return nil
	}))
	stopped := make(chan struct{})
	go func() {
		q.stop()
		close(stopped)
	}()
	receiveDeliveryValue(t, stopped)
	require.ErrorIs(t, receiveDeliveryValue(t, canceled), context.Canceled)
	require.False(t, waitingExecuted.Load())
	require.Zero(t, q.pendingCount())
	require.ErrorIs(t, q.enqueue(deliveryKey{uid: "late"}, func(context.Context) error { return nil }), errDeliveryQueueStopped)
}

func TestDeliveryQueueParentCancellationAndConcurrentStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := newDeliveryQueue(ctx, 4, time.Second)
	t.Cleanup(q.stop)
	t.Cleanup(cancel)
	var producers sync.WaitGroup
	unexpected := make(chan error, 32)
	for uid := range 32 {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for range 100 {
				err := q.enqueue(deliveryKey{uid: fmt.Sprint(uid)}, func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				})
				if err != nil && !errors.Is(err, errDeliveryQueueStopped) {
					unexpected <- err
					return
				}
			}
		}()
	}
	cancel()
	require.ErrorIs(t, q.enqueue(deliveryKey{uid: "after-cancel"}, func(context.Context) error { return nil }), errDeliveryQueueStopped)
	var stops sync.WaitGroup
	for range 4 {
		stops.Add(1)
		go func() { defer stops.Done(); q.stop() }()
	}
	producers.Wait()
	stops.Wait()
	close(unexpected)
	for err := range unexpected {
		require.NoError(t, err)
	}
	require.Zero(t, q.pendingCount())
}

func TestDeliveryQueuePanicRetainsHeadAndOtherKeysContinue(t *testing.T) {
	q := newDeliveryQueue(context.Background(), 1, time.Second)
	t.Cleanup(q.stop)
	firstPanic := make(chan struct{}, 1)
	finished := make(chan string, 3)
	var attempts atomic.Int32
	key := deliveryKey{uid: "panic-user"}
	require.NoError(t, q.enqueue(key, func(context.Context) error {
		n := attempts.Add(1)
		if n == 1 {
			firstPanic <- struct{}{}
		}
		if n <= 2 {
			panic("同一任务临时 panic")
		}
		finished <- "first"
		return nil
	}))
	receiveDeliveryValue(t, firstPanic)
	require.NoError(t, q.enqueue(key, func(context.Context) error {
		finished <- "second"
		return nil
	}))
	require.NoError(t, q.enqueue(deliveryKey{uid: "healthy"}, func(context.Context) error {
		finished <- "healthy"
		return nil
	}))
	require.Equal(t, "healthy", receiveDeliveryValue(t, finished))
	require.Equal(t, "first", receiveDeliveryValue(t, finished))
	require.Equal(t, "second", receiveDeliveryValue(t, finished))
	requireDeliveryQueueEmpty(t, q)
	require.EqualValues(t, 3, attempts.Load())
}

func TestDeliveryQueuePanicErrorIncludesStack(t *testing.T) {
	err, panicked := runDeliveryAttempt(context.Background(), func(context.Context) error {
		panic("投递失败定位标记")
	})
	require.True(t, panicked)
	require.ErrorContains(t, err, "投递失败定位标记")
	require.ErrorContains(t, err, "runtime/debug.Stack")
	require.ErrorContains(t, err, "TestDeliveryQueuePanicErrorIncludesStack")
}

func TestDeliveryQueueCompletedAttemptDoesNotRetryAfterDeadline(t *testing.T) {
	q := newDeliveryQueue(context.Background(), 1, 20*time.Millisecond)
	t.Cleanup(q.stop)
	var attempts atomic.Int32
	finished := make(chan struct{}, 1)
	key := deliveryKey{uid: "completed-at-deadline"}
	require.NoError(t, q.enqueue(key, func(ctx context.Context) error {
		attempts.Add(1)
		<-ctx.Done()
		// 模拟副作用已完成或远端已确认；调度器必须尊重业务返回的成功结果。
		return nil
	}))
	require.NoError(t, q.enqueue(key, func(context.Context) error {
		finished <- struct{}{}
		return nil
	}))
	receiveDeliveryValue(t, finished)
	requireDeliveryQueueEmpty(t, q)
	require.EqualValues(t, 1, attempts.Load())
}
