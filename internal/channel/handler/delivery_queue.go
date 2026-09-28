package handler

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
)

var errDeliveryQueueStopped = errors.New("内存投递队列已经停止")

const (
	deliveryRetryInitial = 100 * time.Millisecond
	deliveryRetryMax     = 5 * time.Second
)

type deliveryKey struct {
	channelId   string
	channelType uint8
	uid         string
}

type deliveryLane struct {
	key         deliveryKey
	attempts    []func(context.Context) error
	readyAt     time.Time
	order       uint64
	retryDelay  time.Duration
	panicLogged bool
}

type deliveryReadyHeap []*deliveryLane

func (h deliveryReadyHeap) Len() int { return len(h) }
func (h deliveryReadyHeap) Less(i, j int) bool {
	if h[i].readyAt.Equal(h[j].readyAt) {
		return h[i].order < h[j].order
	}
	return h[i].readyAt.Before(h[j].readyAt)
}
func (h deliveryReadyHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *deliveryReadyHeap) Push(value any) {
	*h = append(*h, value.(*deliveryLane))
}
func (h *deliveryReadyHeap) Pop() any {
	last := len(*h) - 1
	lane := (*h)[last]
	(*h)[last] = nil
	*h = (*h)[:last]
	return lane
}

type deliveryJob struct {
	lane    *deliveryLane
	attempt func(context.Context) error
}

// deliveryQueue 只保留当前进程的未完成工作；并发有界不代表积压内存有界。
// attempt 必须响应传入的取消信号，队列不会为每条消息创建额外协程。
type deliveryQueue struct {
	mu             sync.Mutex
	lanes          map[deliveryKey]*deliveryLane
	ready          deliveryReadyHeap
	pending        int
	order          uint64
	stopped        bool
	attemptTimeout time.Duration
	cancel         context.CancelFunc
	canceled       <-chan struct{}
	done           chan struct{}
	wake           chan struct{}
	jobs           chan deliveryJob
	workers        sync.WaitGroup
}

func newDeliveryQueue(ctx context.Context, workers int, attemptTimeout time.Duration) *deliveryQueue {
	if workers <= 0 || attemptTimeout <= 0 {
		panic("内存投递队列的并发数和超时必须为正数")
	}
	runCtx, cancel := context.WithCancel(ctx)
	q := &deliveryQueue{
		lanes: make(map[deliveryKey]*deliveryLane), attemptTimeout: attemptTimeout,
		cancel: cancel, canceled: runCtx.Done(), done: make(chan struct{}),
		wake: make(chan struct{}, 1), jobs: make(chan deliveryJob),
	}
	q.workers.Add(workers)
	for range workers {
		go q.work(runCtx)
	}
	go q.schedule(runCtx)
	return q
}

func (q *deliveryQueue) enqueue(key deliveryKey, attempt func(context.Context) error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	select {
	case <-q.canceled:
		return errDeliveryQueueStopped
	default:
	}
	if q.stopped {
		return errDeliveryQueueStopped
	}
	if attempt == nil {
		return errors.New("内存投递任务不能为空")
	}
	lane := q.lanes[key]
	if lane == nil {
		lane = &deliveryLane{key: key}
		q.lanes[key] = lane
		q.makeReady(lane, time.Now())
	}
	lane.attempts = append(lane.attempts, attempt)
	q.pending++
	q.notify()
	return nil
}

func (q *deliveryQueue) stop() {
	q.cancel()
	<-q.done
	q.workers.Wait()
}

func (q *deliveryQueue) pendingCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending
}

// 调用方持有 mu；同一接收者仅在前一次结束后才重新进入就绪堆。
func (q *deliveryQueue) makeReady(lane *deliveryLane, at time.Time) {
	q.order++
	lane.readyAt, lane.order = at, q.order
	heap.Push(&q.ready, lane)
}

func (q *deliveryQueue) notify() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func (q *deliveryQueue) schedule(ctx context.Context) {
	timer := time.NewTimer(time.Hour)
	stopDeliveryTimer(timer)
	defer func() {
		stopDeliveryTimer(timer)
		close(q.jobs)
		q.mu.Lock()
		q.stopped = true
		// 停止不等待积压重试；清除闭包对消息的引用，运行中任务由取消信号结束。
		for _, lane := range q.lanes {
			clear(lane.attempts)
			lane.attempts = nil
		}
		q.lanes, q.ready, q.pending = nil, nil, 0
		q.mu.Unlock()
		close(q.done)
	}()
	for {
		if ctx.Err() != nil {
			return
		}
		var job deliveryJob
		var wait time.Duration
		q.mu.Lock()
		if len(q.ready) > 0 {
			wait = time.Until(q.ready[0].readyAt)
			if wait <= 0 {
				lane := heap.Pop(&q.ready).(*deliveryLane)
				job = deliveryJob{lane: lane, attempt: lane.attempts[0]}
			}
		}
		q.mu.Unlock()
		if job.lane != nil {
			// 不持锁等待工作协程；停止时即使全部工作协程繁忙也能取消。
			select {
			case q.jobs <- job:
			case <-ctx.Done():
				return
			}
			continue
		}
		var timerC <-chan time.Time
		if wait > 0 {
			timer.Reset(wait)
			timerC = timer.C
		}
		select {
		case <-q.wake:
		case <-timerC:
		case <-ctx.Done():
			return
		}
		stopDeliveryTimer(timer)
	}
}

func stopDeliveryTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (q *deliveryQueue) work(ctx context.Context) {
	defer q.workers.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-q.jobs:
			if !ok || ctx.Err() != nil {
				return
			}
			// 每次实际执行重新计时，排队及前一次失败不消耗本次超时。
			attemptCtx, cancel := context.WithTimeout(ctx, q.attemptTimeout)
			err, panicked := runDeliveryAttempt(attemptCtx, job.attempt)
			cancel()
			if panicked && q.firstPanic(job.lane) {
				// 每个队首只报告一次，持续 panic 仍保留待办，但不能刷出重试日志风暴。
				wklog.Error("内存投递任务 panic，保留队首重试", zap.String("channelId", job.lane.key.channelId),
					zap.Uint8("channelType", job.lane.key.channelType), zap.String("uid", job.lane.key.uid), zap.Error(err))
			}
			q.complete(job.lane, err)
		}
	}
}

// 只隔离单次业务调用的 panic，避免新工作协程放大旧事件池中的单任务故障。
func runDeliveryAttempt(ctx context.Context, attempt func(context.Context) error) (err error, panicked bool) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("投递任务 panic: %v\n%s", value, debug.Stack())
			panicked = true
		}
	}()
	// nil 表示业务已经完成；不能在副作用提交后用 context 超时覆盖成功并重复执行。
	err = attempt(ctx)
	return err, false
}

func (q *deliveryQueue) firstPanic(lane *deliveryLane) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if lane.panicLogged {
		return false
	}
	lane.panicLogged = true
	return true
}

func (q *deliveryQueue) complete(lane *deliveryLane, err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.stopped {
		return
	}
	if err == nil {
		lane.attempts[0] = nil
		lane.attempts = lane.attempts[1:]
		q.pending--
		lane.retryDelay = 0
		lane.panicLogged = false
		if len(lane.attempts) == 0 {
			delete(q.lanes, lane.key)
			lane.attempts = nil
			return
		}
	} else if lane.retryDelay == 0 {
		lane.retryDelay = deliveryRetryInitial
	} else {
		lane.retryDelay = min(lane.retryDelay*2, deliveryRetryMax)
	}
	// 成功后下一条、失败后原队首都重新排队，让其他接收者先获得执行机会。
	q.makeReady(lane, time.Now().Add(lane.retryDelay))
	q.notify()
}
