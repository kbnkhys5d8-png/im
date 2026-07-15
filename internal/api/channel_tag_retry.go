package api

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

type channelTagRetryItem struct {
	target     channelTagInvalidationTarget
	generation uint64
	attempts   int
	due        time.Time
	inFlight   bool
	index      int
}

type channelTagRetryHeap []*channelTagRetryItem

func (h channelTagRetryHeap) Len() int { return len(h) }

func (h channelTagRetryHeap) Less(i, j int) bool { return h[i].due.Before(h[j].due) }

func (h channelTagRetryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *channelTagRetryHeap) Push(value any) {
	item := value.(*channelTagRetryItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *channelTagRetryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.index = -1
	*h = old[:last]
	return item
}

type channelTagRetryJob struct {
	target     channelTagInvalidationTarget
	generation uint64
}

// channelTagRetryQueue coalesces invalidations by channel and retries them with
// a fixed worker count. A cluster outage therefore cannot create an unbounded
// number of goroutines or permanently discard a membership change.
type channelTagRetryQueue struct {
	mu          sync.Mutex
	items       map[channelTagInvalidationTarget]*channelTagRetryItem
	pending     channelTagRetryHeap
	jobs        chan channelTagRetryJob
	wake        chan struct{}
	stop        chan struct{}
	startOnce   sync.Once
	stopOnce    sync.Once
	wg          sync.WaitGroup
	workerCount int
	invalidate  func(channelTagInvalidationTarget) error
	backoff     func(int) time.Duration
	stopped     bool
}

func newChannelTagRetryQueue(workerCount int, invalidate func(channelTagInvalidationTarget) error) *channelTagRetryQueue {
	if workerCount < 1 {
		workerCount = 1
	}
	return &channelTagRetryQueue{
		items:       make(map[channelTagInvalidationTarget]*channelTagRetryItem),
		jobs:        make(chan channelTagRetryJob, workerCount),
		wake:        make(chan struct{}, 1),
		stop:        make(chan struct{}),
		workerCount: workerCount,
		invalidate:  invalidate,
		backoff:     channelTagRetryBackoff,
	}
}

func (q *channelTagRetryQueue) Start() {
	q.startOnce.Do(func() {
		q.wg.Add(1 + q.workerCount)
		go q.run()
		for i := 0; i < q.workerCount; i++ {
			go q.runWorker()
		}
	})
}

func (q *channelTagRetryQueue) Stop() {
	q.stopOnce.Do(func() {
		q.mu.Lock()
		q.stopped = true
		q.mu.Unlock()
		close(q.stop)
	})
	q.wg.Wait()
}

func (q *channelTagRetryQueue) Enqueue(target channelTagInvalidationTarget) {
	now := time.Now()
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	item := q.items[target]
	if item == nil {
		item = &channelTagRetryItem{
			target:     target,
			generation: 1,
			due:        now,
			index:      -1,
		}
		q.items[target] = item
		heap.Push(&q.pending, item)
	} else {
		item.generation++
		item.attempts = 0
		item.due = now
		if !item.inFlight {
			if item.index >= 0 {
				heap.Fix(&q.pending, item.index)
			} else {
				heap.Push(&q.pending, item)
			}
		}
	}
	q.mu.Unlock()
	q.notify()
}

func (q *channelTagRetryQueue) Pending() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *channelTagRetryQueue) run() {
	defer q.wg.Done()
	for {
		q.mu.Lock()
		if len(q.pending) == 0 {
			q.mu.Unlock()
			select {
			case <-q.wake:
				continue
			case <-q.stop:
				return
			}
		}

		item := q.pending[0]
		wait := time.Until(item.due)
		if wait > 0 {
			q.mu.Unlock()
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-q.wake:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				continue
			case <-q.stop:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
			continue
		}

		heap.Pop(&q.pending)
		item.inFlight = true
		job := channelTagRetryJob{target: item.target, generation: item.generation}
		q.mu.Unlock()

		select {
		case q.jobs <- job:
		case <-q.stop:
			return
		}
	}
}

func (q *channelTagRetryQueue) runWorker() {
	defer q.wg.Done()
	for {
		select {
		case job := <-q.jobs:
			err := q.runInvalidation(job.target)
			q.complete(job, err)
		case <-q.stop:
			return
		}
	}
}

func (q *channelTagRetryQueue) runInvalidation(target channelTagInvalidationTarget) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("channel tag invalidation panic: %v", recovered)
		}
	}()
	if q.invalidate == nil {
		return fmt.Errorf("channel tag invalidation callback is nil")
	}
	return q.invalidate(target)
}

func (q *channelTagRetryQueue) complete(job channelTagRetryJob, err error) {
	q.mu.Lock()
	item := q.items[job.target]
	if item == nil {
		q.mu.Unlock()
		return
	}
	item.inFlight = false
	if item.generation != job.generation {
		item.attempts = 0
		item.due = time.Now()
		heap.Push(&q.pending, item)
	} else if err == nil {
		delete(q.items, job.target)
	} else {
		item.attempts++
		item.due = time.Now().Add(q.backoff(item.attempts))
		heap.Push(&q.pending, item)
	}
	q.mu.Unlock()
	q.notify()
}

func (q *channelTagRetryQueue) notify() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func channelTagRetryBackoff(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 200 * time.Millisecond
	case 2:
		return time.Second
	case 3:
		return 3 * time.Second
	case 4:
		return 10 * time.Second
	default:
		return 30 * time.Second
	}
}
