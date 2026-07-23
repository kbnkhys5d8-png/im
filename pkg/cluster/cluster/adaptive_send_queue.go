package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// TimerPool Timer 对象池，避免频繁创建 Timer
type TimerPool struct {
	pool sync.Pool
}

// NewTimerPool 创建 Timer 池
func NewTimerPool() *TimerPool {
	return &TimerPool{
		pool: sync.Pool{
			New: func() interface{} {
				return time.NewTimer(0)
			},
		},
	}
}

// Get 从池中获取 Timer
func (tp *TimerPool) Get(timeout time.Duration) *time.Timer {
	timer := tp.pool.Get().(*time.Timer)
	timer.Reset(timeout)
	return timer
}

// Put 将 Timer 放回池中
func (tp *TimerPool) Put(timer *time.Timer) {
	if !timer.Stop() {
		// 如果 timer 已经触发，需要排空 channel
		select {
		case <-timer.C:
		default:
		}
	}
	tp.pool.Put(timer)
}

// 全局 Timer 池
var globalTimerPool = NewTimerPool()

// AdaptiveSendQueue 自适应发送队列，支持动态扩容和优先级
type AdaptiveSendQueue struct {
	// 基础配置
	baseCapacity    int
	maxCapacity     int
	currentCapacity int

	// 队列和锁
	queue  chan *proto.Message
	mu     sync.RWMutex
	closed bool

	// 统计信息
	totalSent     atomic.Uint64
	totalDropped  atomic.Uint64
	totalExpanded atomic.Uint64

	// 性能监控
	avgProcessTime atomic.Uint64 // 平均处理时间（纳秒）
	lastExpandTime time.Time

	// 限流器
	rl *RateLimiter

	// 优先级队列（可选）
	priorityEnabled   bool
	highPriorityQueue chan *proto.Message

	// 消息通知机制
	messageNotify chan struct{} // 用于通知有新消息
	closeNotify   chan struct{} // 用于唤醒阻塞中的接收者
	queueChanged  chan struct{} // 用于广播队列 channel 已替换

	// Timer 池，避免频繁创建 Timer
	timerPool *TimerPool

	wklog.Log
}

// NewAdaptiveSendQueue 创建自适应发送队列
func NewAdaptiveSendQueue(baseCapacity, maxCapacity int, maxSize uint64) *AdaptiveSendQueue {
	if maxCapacity < baseCapacity {
		maxCapacity = baseCapacity * 4 // 默认最大容量是基础容量的4倍
	}

	return &AdaptiveSendQueue{
		baseCapacity:    baseCapacity,
		maxCapacity:     maxCapacity,
		currentCapacity: baseCapacity,
		queue:           make(chan *proto.Message, baseCapacity),
		rl:              NewRateLimiter(maxSize),
		messageNotify:   make(chan struct{}, 1), // 缓冲大小为1的通知channel
		closeNotify:     make(chan struct{}),
		queueChanged:    make(chan struct{}),
		timerPool:       NewTimerPool(), // 初始化 Timer 池
		Log:             wklog.NewWKLog("AdaptiveSendQueue"),
	}
}

// EnablePriority 启用优先级队列
func (asq *AdaptiveSendQueue) EnablePriority(highPriorityCapacity int) {
	asq.mu.Lock()
	defer asq.mu.Unlock()

	if asq.closed {
		return
	}
	asq.priorityEnabled = true
	asq.highPriorityQueue = make(chan *proto.Message, highPriorityCapacity)
	asq.notifyQueueChangedLocked()
	asq.Info("Priority queue enabled", zap.Int("capacity", highPriorityCapacity))
}

// Send 发送消息，支持自动扩容和优先级
func (asq *AdaptiveSendQueue) Send(msg *proto.Message, priority bool) error {
	asq.mu.RLock()
	if asq.closed {
		asq.mu.RUnlock()
		return ErrQueueClosed
	}

	// 检查限流
	if asq.rl.RateLimited() {
		asq.mu.RUnlock()
		asq.Error("Rate limited")
		return ErrRateLimited
	}

	asq.rl.Increase(uint64(msg.Size()))

	// 优先级队列处理
	if asq.priorityEnabled && priority {
		select {
		case asq.highPriorityQueue <- msg:
			asq.mu.RUnlock()
			asq.notifyMessage() // 通知有新消息
			return nil
		default:
			// 高优先级队列满，降级到普通队列
			asq.Warn("High priority queue full, fallback to normal queue")
		}
	}

	// 尝试发送到普通队列
	select {
	case asq.queue <- msg:
		asq.mu.RUnlock()
		asq.totalSent.Inc()
		asq.notifyMessage() // 通知有新消息
		return nil
	default:
		asq.mu.RUnlock()
	}

	// 队列满，尝试扩容。无论是否由当前 goroutine 完成扩容，都要基于
	// 最新队列重试一次，避免写入已经被替换的旧 channel。
	asq.tryExpand()

	asq.mu.RLock()
	if asq.closed {
		asq.mu.RUnlock()
		asq.rl.Decrease(uint64(msg.Size()))
		return ErrQueueClosed
	}
	select {
	case asq.queue <- msg:
		asq.mu.RUnlock()
		asq.totalSent.Inc()
		asq.notifyMessage()
		return nil
	default:
		asq.mu.RUnlock()
		asq.handleDrop(msg)
		return ErrChanIsFull
	}
}

// tryExpand 尝试扩容队列
func (asq *AdaptiveSendQueue) tryExpand() bool {
	asq.mu.Lock()
	defer asq.mu.Unlock()

	if asq.closed {
		return false
	}

	// 检查是否可以扩容
	if asq.currentCapacity >= asq.maxCapacity {
		return false
	}

	// 检查扩容频率限制（避免频繁扩容）
	if time.Since(asq.lastExpandTime) < time.Second {
		return false
	}

	// 计算新容量（每次扩容50%）
	newCapacity := asq.currentCapacity + asq.currentCapacity/2
	if newCapacity > asq.maxCapacity {
		newCapacity = asq.maxCapacity
	}

	// 创建新队列
	newQueue := make(chan *proto.Message, newCapacity)

	// 持有写锁时不会有并发收发，直接迁移当前消息即可。旧 channel
	// 不关闭，避免持有旧引用的 goroutine 出现 send on closed channel。
	oldQueue := asq.queue
	for len(oldQueue) > 0 {
		newQueue <- <-oldQueue
	}

	// 更新队列
	asq.queue = newQueue
	asq.currentCapacity = newCapacity
	asq.lastExpandTime = time.Now()
	asq.totalExpanded.Inc()
	asq.notifyQueueChangedLocked()

	asq.Info("Queue expanded",
		zap.Int("oldCapacity", asq.currentCapacity-asq.currentCapacity/3), // 反推旧容量
		zap.Int("newCapacity", newCapacity))

	return true
}

// handleDrop 处理消息丢弃
func (asq *AdaptiveSendQueue) handleDrop(msg *proto.Message) {
	asq.rl.Decrease(uint64(msg.Size()))
	asq.totalDropped.Inc()

	asq.mu.RLock()
	queueLen := len(asq.queue)
	currentCapacity := asq.currentCapacity
	asq.mu.RUnlock()

	asq.Error("Message dropped due to full queue",
		zap.Uint32("msgType", msg.MsgType),
		zap.Int("queueLen", queueLen),
		zap.Int("capacity", currentCapacity))
}

// Receive 接收消息，优先处理高优先级消息
func (asq *AdaptiveSendQueue) Receive(ctx context.Context) (*proto.Message, bool) {
	return asq.receive(ctx, nil)
}

// BatchReceive 批量接收消息（带超时机制）
func (asq *AdaptiveSendQueue) BatchReceive(ctx context.Context, maxBatchSize int, maxBatchBytes uint64) ([]*proto.Message, bool) {
	msgs := make([]*proto.Message, 0, maxBatchSize)
	totalBytes := uint64(0)

	// 设置批量接收超时时间 - 使用 Timer 池优化
	batchTimeout := time.Millisecond * 100 // 100ms 超时
	timer := asq.timerPool.Get(batchTimeout)
	defer asq.timerPool.Put(timer)

	// 至少尝试获取一个消息（带超时）
	msg, ok := asq.ReceiveWithTimeout(ctx, batchTimeout)
	if !ok {
		return nil, false
	}

	msgs = append(msgs, msg)
	if msg != nil {
		totalBytes += uint64(msg.Size())
	}

	// 尝试获取更多消息（非阻塞）
	for len(msgs) < maxBatchSize && totalBytes < maxBatchBytes {
		asq.mu.RLock()
		if asq.closed {
			asq.mu.RUnlock()
			return msgs, len(msgs) > 0
		}
		select {
		case msg := <-asq.queue:
			asq.mu.RUnlock()
			if msg != nil {
				asq.rl.Decrease(uint64(msg.Size()))
				msgs = append(msgs, msg)
				totalBytes += uint64(msg.Size())
			}
		case msg := <-asq.highPriorityQueue:
			asq.mu.RUnlock()
			if asq.priorityEnabled && msg != nil {
				asq.rl.Decrease(uint64(msg.Size()))
				msgs = append(msgs, msg)
				totalBytes += uint64(msg.Size())
			}
		case <-timer.C:
			asq.mu.RUnlock()
			// 超时，返回当前批次
			return msgs, true
		default:
			asq.mu.RUnlock()
			// 没有更多消息，返回当前批次
			return msgs, true
		}
	}

	return msgs, true
}

// ReceiveWithTimeout 带超时的接收消息（优化版本，避免 time.After 资源泄漏）
func (asq *AdaptiveSendQueue) ReceiveWithTimeout(ctx context.Context, timeout time.Duration) (*proto.Message, bool) {
	timer := asq.timerPool.Get(timeout)
	defer asq.timerPool.Put(timer) // 将 timer 放回池中复用

	return asq.receive(ctx, timer.C)
}

func (asq *AdaptiveSendQueue) receive(ctx context.Context, timeout <-chan time.Time) (*proto.Message, bool) {
	for {
		asq.mu.RLock()
		if asq.closed {
			asq.mu.RUnlock()
			return nil, false
		}
		queue := asq.queue
		queueChanged := asq.queueChanged
		var highPriorityQueue <-chan *proto.Message
		if asq.priorityEnabled {
			highPriorityQueue = asq.highPriorityQueue
		}
		asq.mu.RUnlock()

		// Preserve priority ordering when both queues already contain messages.
		if highPriorityQueue != nil {
			select {
			case msg := <-highPriorityQueue:
				if msg != nil {
					asq.rl.Decrease(uint64(msg.Size()))
				}
				return msg, true
			default:
			}
		}

		select {
		case msg, ok := <-highPriorityQueue:
			if !ok {
				return nil, false
			}
			if msg != nil {
				asq.rl.Decrease(uint64(msg.Size()))
			}
			return msg, true
		case msg, ok := <-queue:
			if !ok {
				return nil, false
			}
			if msg != nil {
				asq.rl.Decrease(uint64(msg.Size()))
			}
			return msg, true
		case <-asq.messageNotify:
			// The queue may have been resized. Refresh channel snapshots.
		case <-queueChanged:
			// Refresh all channel snapshots after a resize or priority change.
		case <-asq.closeNotify:
			return nil, false
		case <-timeout:
			return nil, false
		case <-ctx.Done():
			return nil, false
		}
	}
}

// GetStats 获取队列统计信息
func (asq *AdaptiveSendQueue) GetStats() map[string]interface{} {
	asq.mu.RLock()
	defer asq.mu.RUnlock()

	stats := map[string]interface{}{
		"base_capacity":     asq.baseCapacity,
		"current_capacity":  asq.currentCapacity,
		"max_capacity":      asq.maxCapacity,
		"queue_length":      len(asq.queue),
		"total_sent":        asq.totalSent.Load(),
		"total_dropped":     asq.totalDropped.Load(),
		"total_expanded":    asq.totalExpanded.Load(),
		"rate_limiter_size": asq.rl.Get(),
		"rate_limiter_max":  asq.rl.maxSize,
	}

	if asq.priorityEnabled {
		stats["high_priority_length"] = len(asq.highPriorityQueue)
		stats["priority_enabled"] = true
	}

	return stats
}

// GetUtilization 获取队列利用率
func (asq *AdaptiveSendQueue) GetUtilization() float64 {
	asq.mu.RLock()
	defer asq.mu.RUnlock()

	if asq.currentCapacity == 0 {
		return 0
	}

	return float64(len(asq.queue)) / float64(asq.currentCapacity) * 100
}

// ShouldShrink 检查是否应该缩容
func (asq *AdaptiveSendQueue) ShouldShrink() bool {
	asq.mu.RLock()
	defer asq.mu.RUnlock()

	// 只有当前容量大于基础容量且利用率低于25%时才考虑缩容
	if asq.currentCapacity <= asq.baseCapacity {
		return false
	}

	utilization := float64(len(asq.queue)) / float64(asq.currentCapacity)
	return utilization < 0.25
}

// Shrink 缩容队列
func (asq *AdaptiveSendQueue) Shrink() {
	asq.mu.Lock()
	defer asq.mu.Unlock()

	if asq.closed || asq.currentCapacity <= asq.baseCapacity {
		return
	}

	// 计算新容量（缩容25%）
	newCapacity := asq.currentCapacity * 3 / 4
	if newCapacity < asq.baseCapacity {
		newCapacity = asq.baseCapacity
	}

	// 检查当前消息数量是否适合新容量
	if len(asq.queue) > newCapacity {
		asq.Debug("Cannot shrink: too many messages in queue")
		return
	}

	// 创建新队列并迁移消息
	newQueue := make(chan *proto.Message, newCapacity)
	oldQueue := asq.queue
	for len(oldQueue) > 0 {
		newQueue <- <-oldQueue
	}

	oldCapacity := asq.currentCapacity
	asq.queue = newQueue
	asq.currentCapacity = newCapacity
	asq.notifyQueueChangedLocked()

	asq.Info("Queue shrunk",
		zap.Int("oldCapacity", oldCapacity),
		zap.Int("newCapacity", newCapacity))
}

// Close 关闭队列
func (asq *AdaptiveSendQueue) Close() {
	asq.mu.Lock()
	defer asq.mu.Unlock()

	if asq.closed {
		return
	}
	asq.closed = true
	close(asq.closeNotify)
	close(asq.queue)
	if asq.priorityEnabled {
		close(asq.highPriorityQueue)
	}

	asq.Info("Adaptive send queue closed")
}

// notifyMessage 通知有新消息（非阻塞）
func (asq *AdaptiveSendQueue) notifyMessage() {
	select {
	case asq.messageNotify <- struct{}{}:
		// 通知发送成功
	default:
		// 通知channel已满，忽略（避免阻塞）
	}
}

// notifyQueueChangedLocked broadcasts a structural queue change to every
// receiver. The caller must hold asq.mu for writing.
func (asq *AdaptiveSendQueue) notifyQueueChangedLocked() {
	close(asq.queueChanged)
	asq.queueChanged = make(chan struct{})
}

// WaitForMessage 等待新消息通知（优化版本，使用 Timer 池避免资源泄漏）
func (asq *AdaptiveSendQueue) WaitForMessage(ctx context.Context, timeout time.Duration) bool {
	timer := asq.timerPool.Get(timeout)
	defer asq.timerPool.Put(timer) // 将 timer 放回池中复用

	select {
	case <-asq.messageNotify:
		return true
	case <-timer.C:
		return false
	case <-ctx.Done():
		return false
	}
}
