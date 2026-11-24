package manager

import (
	"math/rand/v2"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

type RetryManager struct {
	retryQueues []*RetryQueue
	timingWheel *timingwheel.TimingWheel
	wklog.Log
}

func NewRetryManager() *RetryManager {
	return &RetryManager{
		retryQueues: make([]*RetryQueue, options.G.MessageRetry.WorkerCount),
		Log:         wklog.NewWKLog("retryManager"),
		timingWheel: timingwheel.NewTimingWheel(options.G.TimingWheelTick, options.G.TimingWheelSize),
	}
}

func (r *RetryManager) Start() error {
	r.timingWheel.Start()
	for i := 0; i < options.G.MessageRetry.WorkerCount; i++ {
		retryQueue := NewRetryQueue(i, r)
		r.retryQueues[i] = retryQueue
		retryQueue.Start()
	}

	return nil
}

func (r *RetryManager) Stop() {

	for i := 0; i < options.G.MessageRetry.WorkerCount; i++ {
		r.retryQueues[i].Stop()
	}

}

// retryMessageCount 获取重试消息数量
func (r *RetryManager) RetryMessageCount() int {
	count := 0
	for i := 0; i < options.G.MessageRetry.WorkerCount; i++ {
		count += r.retryQueues[i].inFlightMessagesCount()
	}
	return count
}

func (r *RetryManager) AddRetry(msg *types.RetryMessage) {
	if q := r.queueForMessage(msg.MessageId); q != nil {
		q.startInFlightTimeout(msg)
	}
}

func (r *RetryManager) RemoveRetry(fromNode uint64, connId int64, messageId int64) error {
	if q := r.queueForMessage(messageId); q != nil {
		q.finishMessage(fromNode, connId, messageId)
	}
	return nil
}

// AcknowledgeRetry 在同一次加锁中验证和移除，防止重复或过期回执删除另一条任务。
func (r *RetryManager) AcknowledgeRetry(uid string, fromNode uint64, connId int64, messageId int64, messageSeq uint32, sessionId string) *types.RetryMessage {
	q := r.queueForMessage(messageId)
	if q == nil || uid == "" || messageSeq == 0 {
		return nil
	}
	key := q.getInFlightKey(fromNode, connId, messageId)
	q.inFlightMutex.Lock()
	defer q.inFlightMutex.Unlock()
	msg := q.inFlightMessages[key]
	if msg == nil || msg.Uid != uid || msg.SessionId != sessionId || msg.RecvPacket == nil ||
		msg.RecvPacket.NoPersist || msg.RecvPacket.MessageID != messageId || msg.RecvPacket.MessageSeq != messageSeq {
		return nil
	}
	q.removeInFlightMessageLocked(key, msg)
	return msg
}

// 获取重试消息
func (r *RetryManager) RetryMessage(fromNode uint64, connId int64, messageId int64) *types.RetryMessage {
	if q := r.queueForMessage(messageId); q != nil {
		return q.getInFlightMessage(fromNode, connId, messageId)
	}
	return nil
}

func (r *RetryManager) queueForMessage(messageId int64) *RetryQueue {
	// 回执的 ID 来自客户端，不能将非正数用于选择重试队列。
	if messageId <= 0 || len(r.retryQueues) == 0 {
		return nil
	}
	return r.retryQueues[messageId%int64(len(r.retryQueues))]
}

func (r *RetryManager) retry(q *RetryQueue, msg *types.RetryMessage, retryCount int) {
	r.Debug("retry msg",
		zap.Int("retryCount", retryCount-1),
		zap.String("uid", msg.Uid),
		zap.Int64("messageId", msg.MessageId),
		zap.Int64("connId", msg.ConnId),
	)
	// 原密文只能发给原会话，连接号复用后不能改投新密钥对应的连接。
	conn := eventbus.User.ConnById(msg.Uid, msg.FromNode, msg.ConnId)
	if conn == nil || conn.SessionId != msg.SessionId {
		if !q.completeRetry(msg, false) {
			return
		}
		r.Debug("conn offline", zap.String("uid", msg.Uid), zap.Int64("messageId", msg.MessageId), zap.Int64("connId", msg.ConnId))

		if options.G.Logger.TraceOn && msg.ChannelType == wkproto.ChannelTypePerson {
			r.Trace("连接已经离线，暂停重试", "retry", zap.String("uid", msg.Uid), zap.Int64("messageId", msg.MessageId), zap.Int64("connId", msg.ConnId))
			conns := eventbus.User.ConnsByUid(msg.Uid)
			if len(conns) > 0 {
				for _, conn := range conns {
					r.Trace("用户名下有其他连接（仅提醒）", "retry", zap.String("uid", msg.Uid), zap.Int64("messageId", msg.MessageId), zap.Int64("connId", conn.ConnId), zap.String("deviceId", conn.DeviceId), zap.String("deviceFlag", conn.DeviceFlag.String()))
				}
			}
		}
		return
	}
	if retryCount > options.G.MessageRetry.MaxCount {
		if !q.completeRetry(msg, false) {
			return
		}
		var messageSeq uint32
		if msg.RecvPacket != nil {
			messageSeq = msg.RecvPacket.MessageSeq
		}
		r.Warn("exceeded the maximum number of retries",
			zap.String("uid", msg.Uid), zap.String("channelId", msg.ChannelId), zap.Uint8("channelType", msg.ChannelType),
			zap.Int64("messageId", msg.MessageId), zap.Uint32("messageSeq", messageSeq),
			zap.Uint64("fromNode", msg.FromNode), zap.Int64("connId", msg.ConnId),
			zap.Int("retryCount", retryCount-1), zap.Int("messageMaxRetryCount", options.G.MessageRetry.MaxCount))
		if options.G.Logger.TraceOn && msg.ChannelType == wkproto.ChannelTypePerson {
			r.Trace("超过最大重试次数，暂停重试", "retry",
				zap.String("uid", msg.Uid),
				zap.Int64("messageId", msg.MessageId),
				zap.Int64("connId", msg.ConnId),
				zap.Int("retryCount", retryCount),
				zap.Int("maxRetryCount", options.G.MessageRetry.MaxCount),
			)
		}
		return
	}
	// 重挂前再次确认仍是原对象；网络发送不持有队列锁。
	if !q.completeRetry(msg, true) {
		return
	}

	if options.G.Logger.TraceOn && msg.ChannelType == wkproto.ChannelTypePerson {
		r.Trace("重试消息", "retry",
			zap.String("uid", msg.Uid),
			zap.Int64("messageId", msg.MessageId),
			zap.Int64("connId", msg.ConnId),
			zap.String("deviceId", conn.DeviceId),
			zap.String("deviceFlag", conn.DeviceFlag.String()),
			zap.Int("retryCount", retryCount),
			zap.Int("maxRetryCount", options.G.MessageRetry.MaxCount),
		)
	}

	// 发送消息
	// 在需要打印日志的地方添加概率控制
	if rand.Float64() < 0.1 { // 10%的概率
		r.Info("retry send message",
			zap.Int("retry", retryCount),
			zap.Uint64("fromNode", msg.FromNode),
			zap.String("uid", msg.Uid),
			zap.Int64("messageId", msg.MessageId),
			zap.Int64("connId", msg.ConnId),
		)
	}

	eventbus.User.ConnWrite("", conn, msg.RecvPacket)

}

// Schedule 延迟任务
func (r *RetryManager) schedule(interval time.Duration, f func()) *timingwheel.Timer {
	return r.timingWheel.ScheduleFunc(&everyScheduler{
		Interval: interval,
	}, f)
}

type everyScheduler struct {
	Interval time.Duration
}

func (s *everyScheduler) Next(prev time.Time) time.Time {
	return prev.Add(s.Interval)
}
