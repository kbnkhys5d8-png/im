package service

import "github.com/WuKongIM/WuKongIM/internal/types"

var RetryManager RetryMgr

type RetryMgr interface {
	// RetryMessageCount 重试消息数量
	RetryMessageCount() int
	// AddRetry 添加重试消息
	AddRetry(msg *types.RetryMessage)
	// RemoveRetry 移除重试消息
	RemoveRetry(fromNode uint64, connId int64, messageId int64) error
	// AcknowledgeRetry 核对收件连接与服务端原始消息后，原子地移除重试任务。
	AcknowledgeRetry(uid string, fromNode uint64, connId int64, messageId int64, messageSeq uint32) *types.RetryMessage
	// 获取重试消息
	RetryMessage(fromNode uint64, connId int64, messageId int64) *types.RetryMessage
}
