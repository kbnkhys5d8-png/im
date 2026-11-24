package manager

import (
	"testing"
	"time"

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

func TestAcknowledgeRetryMatchesSentMessageAndOnlyConsumesOnce(t *testing.T) {
	m, q := newRetryQueueTest(t)
	m.retryQueues = append(m.retryQueues, NewRetryQueue(1, m))
	msg := retryQueueTestMessage(1, 8, 10)
	msg.RecvPacket.MessageSeq = 20
	sibling := retryQueueTestMessage(1, 9, 10)
	sibling.RecvPacket.MessageSeq = 20
	m.AddRetry(msg)
	m.AddRetry(sibling)
	for _, tc := range []struct {
		name      string
		uid       string
		nodeId    uint64
		connId    int64
		messageId int64
		seq       uint32
	}{
		{name: "非正数消息ID", uid: msg.Uid, nodeId: 1, connId: 8, messageId: -1, seq: 20},
		{name: "其他用户", uid: "another-user", nodeId: 1, connId: 8, messageId: 10, seq: 20},
		{name: "其他节点", uid: msg.Uid, nodeId: 2, connId: 8, messageId: 10, seq: 20},
		{name: "其他连接", uid: msg.Uid, nodeId: 1, connId: 7, messageId: 10, seq: 20},
		{name: "无序列", uid: msg.Uid, nodeId: 1, connId: 8, messageId: 10, seq: 0},
		{name: "序列不匹配", uid: msg.Uid, nodeId: 1, connId: 8, messageId: 10, seq: 21},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := m.AcknowledgeRetry(tc.uid, tc.nodeId, tc.connId, tc.messageId, tc.seq, msg.SessionId); got != nil || q.inFlightMessagesCount() != 2 {
				t.Fatal("不匹配的回执不能移除任务")
			}
		})
	}
	results := make(chan *types.RetryMessage, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			results <- m.AcknowledgeRetry(msg.Uid, msg.FromNode, msg.ConnId, msg.MessageId, msg.RecvPacket.MessageSeq, msg.SessionId)
		}()
	}
	close(start)
	first, second := <-results, <-results
	if !((first == msg && second == nil) || (first == nil && second == msg)) {
		t.Fatal("并发或重复的合法回执只能移除一次")
	}
	if m.RetryMessage(sibling.FromNode, sibling.ConnId, sibling.MessageId) != sibling || q.inFlightMessagesCount() != 1 {
		t.Fatal("另一连接的重试必须保留")
	}
	// 保留的内部查询和移除入口也不能把外部 ID 变成负数组下标。
	if m.RetryMessage(1, 8, -1) != nil {
		t.Fatal("非正数 ID 不应返回任务")
	}
	if err := m.RemoveRetry(1, 8, -1); err != nil {
		t.Fatalf("非正数 ID 应为空操作: %v", err)
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
			msg.RecvPacket.MessageSeq = 20
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
			if got := m.AcknowledgeRetry(msg.Uid, msg.FromNode, msg.ConnId, msg.MessageId, msg.RecvPacket.MessageSeq, msg.SessionId); got != msg {
				t.Error("扫描中的任务仍应可确认")
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

func TestRetryQueueLimitDistinguishesClosedConnection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		missing bool
	}{
		{name: "原连接已关闭", missing: true},
		{name: "原连接仍存在"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, q := newRetryQueueTest(t)
			probe := registerRetryQueueTestUser(t)
			probe.connMissing = tc.missing
			log := &retryQueueTestLog{Log: m.Log}
			m.Log = log
			msg := retryQueueTestMessage(1, 8, 10)
			msg.Retry = options.G.MessageRetry.MaxCount
			msg.RecvPacket.MessageSeq = 20
			m.AddRetry(msg)
			msg.Pri = 1
			q.processInFlightQueue(1)
			if len(probe.frames) != 0 || q.inFlightMessagesCount() != 0 {
				t.Fatal("达到上限的任务必须清理，不能额外发消息")
			}
			if tc.missing {
				if len(log.warnings) != 0 {
					t.Fatal("原连接已关闭时不能误报仍在线连接的重试耗尽")
				}
				return
			}
			if len(log.warnings) != 1 {
				t.Fatalf("原连接仍存在时必须保留重试耗尽告警，实际 %d 条", len(log.warnings))
			}
			got := make(map[string]int64)
			for _, field := range log.warningFields[0] {
				got[field.Key] = field.Integer
			}
			for key, want := range map[string]int64{"fromNode": 1, "connId": 8, "messageSeq": 20, "retryCount": int64(options.G.MessageRetry.MaxCount)} {
				if got[key] != want {
					t.Errorf("诊断字段 %s=%d，期望 %d", key, got[key], want)
				}
			}
		})
	}
}

func TestRetryQueueLimitLookupStillAcceptsConcurrentAck(t *testing.T) {
	m, q := newRetryQueueTest(t)
	probe := registerRetryQueueTestUser(t)
	log := &retryQueueTestLog{Log: m.Log}
	m.Log = log
	msg := retryQueueTestMessage(1, 8, 10)
	msg.Retry = options.G.MessageRetry.MaxCount
	msg.RecvPacket.MessageSeq = 20
	m.AddRetry(msg)
	msg.Pri = 1
	started, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	probe.onLookup = func() {
		close(started)
		<-resume
	}
	go func() {
		defer close(done)
		q.processInFlightQueue(1)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(resume)
		t.Fatal("连接查询未开始")
	}
	// 查询原连接时仍保留任务，迟到的合法 ACK 可以抢先完成确认。
	if got := m.AcknowledgeRetry(msg.Uid, msg.FromNode, msg.ConnId, msg.MessageId, msg.RecvPacket.MessageSeq, msg.SessionId); got != msg {
		t.Error("上限检查期间的合法 ACK 不能丢失")
	}
	close(resume)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("重试扫描未完成")
	}
	if len(log.warnings) != 0 || len(probe.frames) != 0 || q.inFlightMessagesCount() != 0 {
		t.Fatal("已经确认的消息不能误报告警、重新入队或再次发送")
	}
}

func TestRetryQueueNeverReplaysCiphertextToReusedConnection(t *testing.T) {
	for _, tc := range []struct {
		name        string
		original    string
		current     string
		wantWrites  int
		wantPending int
	}{
		{name: "原会话继续重试", original: "session-1", current: "session-1", wantWrites: 1, wantPending: 1},
		{name: "连接号复用不能转投新会话", original: "session-1", current: "session-2"},
		{name: "原会话不可降级为空代次", original: "session-1"},
		{name: "旧节点会话不能转投新会话", current: "session-2"},
		{name: "旧节点同会话保持兼容", wantWrites: 1, wantPending: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, q := newRetryQueueTest(t)
			probe := registerRetryQueueTestUser(t)
			probe.sessionID = tc.current
			msg := retryQueueTestMessage(1, 8, 10)
			msg.SessionId = tc.original
			msg.RecvPacket.Payload = []byte("original-session-ciphertext")
			m.AddRetry(msg)
			msg.Pri = 1
			q.processInFlightQueue(1)
			if len(probe.frames) != tc.wantWrites || q.inFlightMessagesCount() != tc.wantPending {
				t.Fatalf("会话重试结果错误：写入=%d 待确认=%d", len(probe.frames), q.inFlightMessagesCount())
			}
		})
	}
}

func TestRetryQueueCurrentSessionReplacesOnlyOldGeneration(t *testing.T) {
	for _, oldArrivesLast := range []bool{false, true} {
		name := "新会话替换残留任务"
		if oldArrivesLast {
			name = "迟到旧任务不得覆盖新会话"
		}
		t.Run(name, func(t *testing.T) {
			m, q := newRetryQueueTest(t)
			probe := registerRetryQueueTestUser(t)
			probe.sessionID = "new-session"
			old := retryQueueTestMessage(1, 8, 10)
			old.SessionId = "old-session"
			current := retryQueueTestMessage(1, 8, 10)
			current.SessionId = "new-session"
			if oldArrivesLast {
				m.AddRetry(current)
				m.AddRetry(old)
			} else {
				m.AddRetry(old)
				m.AddRetry(current)
			}
			if m.RetryMessage(1, 8, 10) != current || q.inFlightMessagesCount() != 1 {
				t.Fatal("同一消息和连接号只能保留当前会话的任务")
			}
			current.Pri = 1
			q.processInFlightQueue(1)
			if len(probe.frames) != 1 || probe.frames[0] != current.RecvPacket {
				t.Fatal("新会话必须能重试，不能发出旧会话密文")
			}
		})
	}
}

func TestAcknowledgeRetryRejectsPreviousSessionOnReusedConnection(t *testing.T) {
	m, q := newRetryQueueTest(t)
	msg := retryQueueTestMessage(1, 8, 10)
	msg.SessionId = "new-session"
	msg.RecvPacket.MessageSeq = 20
	m.AddRetry(msg)
	for _, sessionID := range []string{"old-session", ""} {
		if got := m.AcknowledgeRetry(msg.Uid, msg.FromNode, msg.ConnId, msg.MessageId, 20, sessionID); got != nil || q.inFlightMessagesCount() != 1 {
			t.Fatal("旧会话的相同消息回执不能移除新会话的任务")
		}
	}
	if got := m.AcknowledgeRetry(msg.Uid, msg.FromNode, msg.ConnId, msg.MessageId, 20, "new-session"); got != msg || q.inFlightMessagesCount() != 0 {
		t.Fatal("原协议的当前会话回执必须正常确认")
	}
}

func TestRetryQueueSessionReplacementPreservesConcurrentCompletion(t *testing.T) {
	for _, finishWithAck := range []bool{false, true} {
		name := "旧重试已出堆"
		if finishWithAck {
			name = "替换校验期间收到旧回执"
		}
		t.Run(name, func(t *testing.T) {
			m, q := newRetryQueueTest(t)
			probe := registerRetryQueueTestUser(t)
			probe.sessionID = "new-session"
			old := retryQueueTestMessage(1, 8, 10)
			old.SessionId = "old-session"
			old.RecvPacket.MessageSeq = 20
			current := retryQueueTestMessage(1, 8, 10)
			current.SessionId = "new-session"
			current.RecvPacket.MessageSeq = 20
			m.AddRetry(old)
			ackResult := make(chan *types.RetryMessage, 1)
			probe.onLookup = func() {
				probe.onLookup = nil
				if finishWithAck {
					ackResult <- m.AcknowledgeRetry(old.Uid, 1, 8, 10, 20, old.SessionId)
				} else {
					m.AddRetry(current)
				}
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				if finishWithAck {
					m.AddRetry(current)
				} else {
					old.Pri = 1
					q.processInFlightQueue(1)
				}
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("会话替换与队列完成不能互相死锁")
			}
			if finishWithAck {
				select {
				case got := <-ackResult:
					if got != old {
						t.Fatal("替换登记前的原会话回执应当正常完成")
					}
				default:
					t.Fatal("未执行并发回执校验")
				}
			}
			if m.RetryMessage(1, 8, 10) != current || q.inFlightMessagesCount() != 1 {
				t.Fatal("旧任务完成后必须保留新会话任务")
			}
			current.Pri = 2
			q.processInFlightQueue(2)
			if len(probe.frames) != 1 || probe.frames[0] != current.RecvPacket {
				t.Fatal("只能重试新会话的下行包")
			}
		})
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
	onLookup    func()
	connMissing bool
	sessionID   string
	frames      []wkproto.Frame
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
	if p.connMissing {
		return nil
	}
	return &eventbus.Conn{Uid: uid, NodeId: nodeID, ConnId: connID, SessionId: p.sessionID}
}

func (p *retryQueueTestUser) AddEvent(_ string, event *eventbus.Event) {
	p.frames = append(p.frames, event.Frame)
}

type retryQueueTestLog struct {
	wklog.Log
	warnings      []string
	warningFields [][]zap.Field
}

func (l *retryQueueTestLog) Warn(msg string, fields ...zap.Field) {
	l.warnings = append(l.warnings, msg)
	l.warningFields = append(l.warningFields, append([]zap.Field(nil), fields...))
}
