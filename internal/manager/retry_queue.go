package manager

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/valyala/fastrand"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// RetryQueue 重试队列
type RetryQueue struct {
	inFlightPQ       inFlightPqueue
	inFlightMessages map[string]*types.RetryMessage
	inFlightMutex    sync.Mutex
	fakeMessageID    int64
	wklog.Log
	r          *RetryManager
	stopped    atomic.Bool
	retryTimer *timingwheel.Timer
}

// NewRetryQueue NewRetryQueue
func NewRetryQueue(index int, r *RetryManager) *RetryQueue {

	return &RetryQueue{
		r:                r,
		inFlightPQ:       newInFlightPqueue(4056),
		inFlightMessages: make(map[string]*types.RetryMessage),
		fakeMessageID:    10000,
		Log:              wklog.NewWKLog(fmt.Sprintf("RetryQueue[%d]", index)),
	}
}

func (r *RetryQueue) startInFlightTimeout(msg *types.RetryMessage) {
	key := r.getInFlightKey(msg.FromNode, msg.ConnId, msg.MessageId)
	r.inFlightMutex.Lock()
	if r.inFlightMessages[key] != nil {
		r.inFlightMutex.Unlock()
		r.Warn("ID already in flight",
			zap.String("key", key),
			zap.String("uid", msg.Uid),
			zap.Uint64("fromNode", msg.FromNode),
			zap.Int64("connId", msg.ConnId),
			zap.Int64("messageId", msg.MessageId),
		)
		return
	}
	// 表和堆一起登记，避免回执移除了表项后仍留下堆项。
	r.inFlightMessages[key] = msg
	msg.Pri = time.Now().Add(options.G.MessageRetry.Interval).UnixNano()
	r.inFlightPQ.Push(msg)
	r.inFlightMutex.Unlock()
}

func (r *RetryQueue) getInFlightMessage(fromNodeId uint64, connId int64, messageId int64) *types.RetryMessage {
	r.inFlightMutex.Lock()
	defer r.inFlightMutex.Unlock()
	key := r.getInFlightKey(fromNodeId, connId, messageId)
	msg := r.inFlightMessages[key]
	return msg
}

func (r *RetryQueue) getInFlightKey(fromNodeId uint64, connId int64, messageId int64) string {
	var b strings.Builder
	b.WriteString(strconv.FormatUint(fromNodeId, 10))
	b.WriteString(":")
	b.WriteString(strconv.FormatInt(connId, 10))
	b.WriteString(":")
	b.WriteString(strconv.FormatInt(messageId, 10))
	return b.String()
}
func (r *RetryQueue) finishMessage(fromNode uint64, connId int64, messageId int64) {
	key := r.getInFlightKey(fromNode, connId, messageId)
	r.inFlightMutex.Lock()
	defer r.inFlightMutex.Unlock()
	msg := r.inFlightMessages[key]
	if msg == nil {
		// 重复或迟到的回执无需再次移除，不视为失败。
		return
	}
	r.removeInFlightMessageLocked(key, msg)
}

// removeInFlightMessageLocked 只在持有队列锁且已确认任务对象时调用。
func (r *RetryQueue) removeInFlightMessageLocked(key string, msg *types.RetryMessage) {
	delete(r.inFlightMessages, key)
	if msg.Index >= 0 {
		r.inFlightPQ.Remove(msg.Index)
	}
}

func (r *RetryQueue) takeDueRetry(t int64) (*types.RetryMessage, int) {
	r.inFlightMutex.Lock()
	defer r.inFlightMutex.Unlock()
	for {
		msg, _ := r.inFlightPQ.PeekAndShift(t)
		if msg == nil {
			return nil, 0
		}
		key := r.getInFlightKey(msg.FromNode, msg.ConnId, msg.MessageId)
		if r.inFlightMessages[key] != msg {
			// 旧堆项不能触碰新对象，也不能阻塞后续到期任务。
			continue
		}
		// 出堆后仍留在表内，查询连接期间回执仍可移除它。
		msg.Retry++
		return msg, msg.Retry
	}
}

func (r *RetryQueue) completeRetry(msg *types.RetryMessage, rearm bool) bool {
	key := r.getInFlightKey(msg.FromNode, msg.ConnId, msg.MessageId)
	r.inFlightMutex.Lock()
	defer r.inFlightMutex.Unlock()
	if r.inFlightMessages[key] != msg || msg.Index >= 0 {
		return false
	}
	// 回执已经移除或替换的对象既不能重挂，也不能删除新任务。
	if rearm {
		msg.Pri = time.Now().Add(options.G.MessageRetry.Interval).UnixNano()
		r.inFlightPQ.Push(msg)
		return true
	}
	delete(r.inFlightMessages, key)
	return true
}

func (r *RetryQueue) processInFlightQueue(t int64) {
	for !r.stopped.Load() {
		msg, retryCount := r.takeDueRetry(t)
		if msg == nil {
			return
		}
		r.r.retry(r, msg, retryCount)
	}
}

// inFlightMessagesCount 返回正在飞行的消息数量
func (r *RetryQueue) inFlightMessagesCount() int {
	r.inFlightMutex.Lock()
	defer r.inFlightMutex.Unlock()
	return max(len(r.inFlightMessages), len(r.inFlightPQ))
}

// Start 开始运行重试
func (r *RetryQueue) Start() {

	scanInterval := options.G.MessageRetry.ScanInterval

	p := float64(fastrand.Uint32()) / (1 << 32)
	// 以避免系统中因定时器、周期性任务或请求间隔完全一致而导致的同步问题（例如拥堵或资源竞争）。
	jitter := time.Duration(p * float64(scanInterval))
	r.retryTimer = r.r.schedule(scanInterval+jitter, func() {
		now := time.Now().UnixNano()
		r.processInFlightQueue(now)
	})
}

func (r *RetryQueue) Stop() {
	r.stopped.Store(true)
	if r.retryTimer != nil {
		r.retryTimer.Stop()
		r.retryTimer = nil
	}
}
