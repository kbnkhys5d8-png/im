package manager

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

func TestRemoveRetryIsIdempotentAndConnectionScoped(t *testing.T) {
	m, q := newRetryQueueTest(t)
	log := &retryQueueTestLog{Log: q.Log}
	q.Log = log
	first := retryQueueTestMessage(1, 8, 10)
	sibling := retryQueueTestMessage(2, 8, 10)
	m.AddRetry(first)
	m.AddRetry(sibling)
	for range 2 {
		if err := m.RemoveRetry(first.FromNode, first.ConnId, first.MessageId); err != nil {
			t.Errorf("移除同一回执应当幂等: %v", err)
		}
	}
	if len(log.warnings) != 0 {
		t.Errorf("重复回执不应告警: %v", log.warnings)
	}
	if m.RetryMessage(sibling.FromNode, sibling.ConnId, sibling.MessageId) != sibling || q.inFlightMessagesCount() != 1 {
		t.Fatal("确认一个连接后必须只保留其他连接的任务")
	}
}

func TestRetryQueueSkipsStaleEntriesWithoutDelayingLaterMessages(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		name := "已移除"
		if replaced {
			name = "已替换"
		}
		t.Run(name, func(t *testing.T) {
			m, q := newRetryQueueTest(t)
			probe := registerRetryQueueTestUser(t)
			stale := retryQueueTestMessage(1, 8, 10)
			replacement := retryQueueTestMessage(1, 8, 10)
			if replaced {
				m.AddRetry(replacement)
			}
			// 模拟扫描遇到不再拥有该键的旧堆项，后面的到期任务不能被挡住。
			stale.Pri = 1
			q.inFlightPQ.Push(stale)
			later := retryQueueTestMessage(1, 8, 11)
			m.AddRetry(later)
			later.Pri = 2
			q.processInFlightQueue(2)
			if len(probe.frames) != 1 || probe.frames[0] != later.RecvPacket {
				t.Errorf("只应重发后续到期任务，实际发送 %d 次", len(probe.frames))
			}
			var expected *types.RetryMessage
			if replaced {
				expected = replacement
			}
			if got := m.RetryMessage(stale.FromNode, stale.ConnId, stale.MessageId); got != expected {
				t.Error("旧堆项不能删除或重挂该键的任务")
			}
		})
	}
}

func TestRetryQueueAckWhileResendingDoesNotRearm(t *testing.T) {
	for _, replaced := range []bool{false, true} {
		name := "收到回执"
		if replaced {
			name = "收到回执后同键登记新对象"
		}
		t.Run(name, func(t *testing.T) {
			m, q := newRetryQueueTest(t)
			probe := registerRetryQueueTestUser(t)
			msg := retryQueueTestMessage(1, 8, 10)
			replacement := retryQueueTestMessage(1, 8, 10)
			m.AddRetry(msg)
			msg.Pri = 1
			// 连接查询位于出堆之后、重挂之前，回执按固定顺序进入，无需依赖定时休眠。
			lookupStarted := make(chan struct{})
			resumeLookup := make(chan struct{})
			scanDone := make(chan struct{})
			probe.onLookup = func() {
				close(lookupStarted)
				<-resumeLookup
			}
			go func() {
				defer close(scanDone)
				q.processInFlightQueue(1)
			}()
			<-lookupStarted
			if err := m.RemoveRetry(msg.FromNode, msg.ConnId, msg.MessageId); err != nil {
				t.Errorf("扫描中的任务仍应可确认: %v", err)
			}
			if replaced {
				m.AddRetry(replacement)
			}
			close(resumeLookup)
			<-scanDone
			if len(probe.frames) != 0 {
				t.Error("已确认的旧对象不应重发")
			}
			var expected *types.RetryMessage
			if replaced {
				expected = replacement
			}
			if got := m.RetryMessage(msg.FromNode, msg.ConnId, msg.MessageId); got != expected {
				t.Error("已确认的对象不应重挂或替换新对象")
			}
		})
	}
}

func TestRetryQueueRetainsRetriesAndLimit(t *testing.T) {
	m, q := newRetryQueueTest(t)
	probe := registerRetryQueueTestUser(t)
	log := &retryQueueTestLog{Log: m.Log}
	m.Log = log
	msg := retryQueueTestMessage(1, 8, 10)
	msg.Retry = options.G.MessageRetry.MaxCount - 1
	m.AddRetry(msg)
	msg.Pri = 1
	q.processInFlightQueue(1)
	if len(probe.frames) != 1 || probe.frames[0] != msg.RecvPacket || q.inFlightMessagesCount() != 1 {
		t.Fatal("尚未达到上限且未确认的任务必须正常重发、重挂")
	}
	msg.Pri = 1
	q.processInFlightQueue(1)
	if len(probe.frames) != 1 || q.inFlightMessagesCount() != 0 {
		t.Fatal("达到重试上限后必须停止重发并清理任务")
	}
	if len(log.warnings) != 1 || log.warnings[0] != "exceeded the maximum number of retries" {
		t.Errorf("必须保留重试上限告警: %v", log.warnings)
	}
}

func newRetryQueueTest(t *testing.T) (*RetryManager, *RetryQueue) {
	t.Helper()
	// 只构造一条队列，手动推进扫描，不启动后台定时器。
	m := &RetryManager{retryQueues: make([]*RetryQueue, 1), Log: wklog.NewWKLog("retryTest")}
	m.retryQueues[0] = NewRetryQueue(0, m)
	return m, m.retryQueues[0]
}

func retryQueueTestMessage(nodeID uint64, connID, messageID int64) *types.RetryMessage {
	return &types.RetryMessage{
		FromNode:   nodeID,
		ConnId:     connID,
		MessageId:  messageID,
		Uid:        "retry-test-user",
		RecvPacket: &wkproto.RecvPacket{MessageID: messageID},
	}
}

type retryQueueTestUser struct {
	eventbus.IUser
	onLookup func()
	frames   []wkproto.Frame
}

func registerRetryQueueTestUser(t *testing.T) *retryQueueTestUser {
	t.Helper()
	old := eventbus.User
	probe := &retryQueueTestUser{}
	eventbus.RegisterUser(probe)
	t.Cleanup(func() { eventbus.User = old })
	return probe
}

func (p *retryQueueTestUser) ConnById(uid string, nodeID uint64, connID int64) *eventbus.Conn {
	if p.onLookup != nil {
		p.onLookup()
	}
	return &eventbus.Conn{Uid: uid, NodeId: nodeID, ConnId: connID}
}

func (p *retryQueueTestUser) AddEvent(_ string, event *eventbus.Event) {
	p.frames = append(p.frames, event.Frame)
}

type retryQueueTestLog struct {
	wklog.Log
	warnings []string
}

func (l *retryQueueTestLog) Warn(msg string, _ ...zap.Field) {
	l.warnings = append(l.warnings, msg)
}
