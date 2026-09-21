package webhook

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type failureStoreStub struct {
	mu        sync.Mutex
	values    map[int64]wkdb.Message
	saveErr   error
	saveErrID map[int64]error
	removeErr error
	hasErr    error
	saved     chan struct{}
	queue     *notifyQueueStub
}

func (s *failureStoreStub) Save(messages []wkdb.Message, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.queue != nil && s.queue.closed.Load() {
		return errors.New("queue closed before archive completed")
	}
	if s.saved != nil {
		select {
		case s.saved <- struct{}{}:
		default:
		}
	}
	if s.saveErr != nil {
		return s.saveErr
	}
	for _, message := range messages {
		if err := s.saveErrID[message.MessageID]; err != nil {
			return err
		}
		s.values[message.MessageID] = message
	}
	return nil
}

func (s *failureStoreStub) Has(id int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.values[id]
	return exists, s.hasErr
}

func (s *failureStoreStub) Remove(ids []int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.removeErr != nil {
		return s.removeErr
	}
	for _, id := range ids {
		delete(s.values, id)
	}
	return nil
}

func newReliabilityWebhook(t *testing.T, transport notifyRoundTripper) (*Webhook, *failureStoreStub) {
	t.Helper()
	previous := options.G
	options.G = &options.Options{}
	options.G.Webhook.HTTPAddr = "http://notify.invalid/webhook"
	options.G.Webhook.MsgNotifyEventRetryMaxCount = 3
	options.G.Webhook.MsgNotifyEventCountPerPush = 2
	options.G.Webhook.MsgNotifyEventPushInterval = time.Millisecond * 10
	t.Cleanup(func() { options.G = previous })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	store := &failureStoreStub{values: make(map[int64]wkdb.Message)}
	return &Webhook{
		Log: silentWebhookLog{}, failures: store, stoped: make(chan struct{}),
		requestCtx: ctx, cancelRequests: cancel, httpClient: &http.Client{Transport: transport},
	}, store
}

func notifyTestMessage(id int64) wkdb.Message {
	return wkdb.Message{RecvPacket: wkproto.RecvPacket{
		MessageID: id, MessageSeq: 1, ChannelID: "test", ChannelType: wkproto.ChannelTypeGroup,
		Payload: []byte(`{"type":1,"content":"仅供本地测试"}`),
	}}
}

func notifyResponse(req *http.Request, status int) *http.Response {
	return &http.Response{
		StatusCode: status, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header), Request: req,
	}
}

func TestFailedNotifyPausesAfterLimitAndRetainsEvidence(t *testing.T) {
	attempts := 0
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		attempts++
		return notifyResponse(req, http.StatusServiceUnavailable), nil
	})
	pending := []wkdb.Message{notifyTestMessage(11)}
	counts := make(map[int64]int)
	for i := 0; i < 3; i++ {
		ok, next := w.pushMessages(pending, counts)
		if ok {
			t.Fatal("暂停人工核验不等于投递成功")
		}
		pending = next
		if exists, _ := store.Has(11); !exists {
			t.Fatal("首次失败即须保留通知")
		}
	}
	if len(pending) != 0 || attempts != 3 || len(counts) != 0 {
		t.Fatalf("达到上限后未暂停：pending=%d attempts=%d counts=%d", len(pending), attempts, len(counts))
	}
	// 模拟旧磁盘游标重读到同一条消息，不得自动重放已有失败证据。
	w.pushMessages([]wkdb.Message{notifyTestMessage(11)}, counts)
	if attempts != 3 {
		t.Fatal("已有失败记录被自动补发")
	}
}

func TestArchiveFailureBlocksFurtherHTTP(t *testing.T) {
	attempts := 0
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		attempts++
		return notifyResponse(req, http.StatusServiceUnavailable), nil
	})
	store.saveErr = errors.New("disk full")
	pending := []wkdb.Message{notifyTestMessage(12)}
	counts := make(map[int64]int)
	for i := 0; i < 5; i++ {
		_, pending = w.pushMessages(pending, counts)
		if len(pending) != 1 {
			t.Fatal("归档失败不能丢弃内存记录")
		}
	}
	if attempts != 1 {
		t.Fatalf("归档未完成却再次请求 HTTP：%d", attempts)
	}
}

func TestSuccessfulRetryDoesNotResendOnArchiveRemovalFailure(t *testing.T) {
	for _, removeFails := range []bool{false, true} {
		t.Run(map[bool]string{false: "remove_success", true: "remove_failure"}[removeFails], func(t *testing.T) {
			attempts := 0
			w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
				attempts++
				status := http.StatusOK
				if attempts == 1 {
					status = http.StatusServiceUnavailable
				}
				return notifyResponse(req, status), nil
			})
			if removeFails {
				store.removeErr = errors.New("directory sync failed")
			}
			counts := make(map[int64]int)
			_, pending := w.pushMessages([]wkdb.Message{notifyTestMessage(13)}, counts)
			ok, pending := w.pushMessages(pending, counts)
			if !ok || len(pending) != 0 || attempts != 2 {
				t.Fatal("远端成功不能因本地清理失败而再次发送")
			}
			exists, _ := store.Has(13)
			if exists != removeFails {
				t.Fatal("失败证据保留状态不正确")
			}
		})
	}
}

func TestArchiveFailureAlsoBlocksUnsentAgentBatch(t *testing.T) {
	var hosts []string
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		hosts = append(hosts, req.URL.Host)
		return notifyResponse(req, http.StatusServiceUnavailable), nil
	})
	options.G.Agent.Webhook.HTTPAddr = "http://agent.invalid/webhook"
	store.saveErr = errors.New("disk full")
	agent := notifyTestMessage(32)
	agent.ChannelType = wkproto.ChannelTypeAgent
	pending := []wkdb.Message{notifyTestMessage(31), agent}
	counts := make(map[int64]int)
	for i := 0; i < 3; i++ {
		_, pending = w.pushMessages(pending, counts)
	}
	if len(hosts) != 1 || hosts[0] != "notify.invalid" || len(pending) != 2 {
		t.Fatalf("普通批次未保留时仍继续发送：hosts=%v pending=%d", hosts, len(pending))
	}
	store.saveErr = nil
	w.pushMessages(pending, counts)
	if len(hosts) != 3 || hosts[2] != "agent.invalid" {
		t.Fatalf("证据归档恢复后才允许继续有限重试：%v", hosts)
	}
}

func TestRetryLimitOneWaitsForArchiveWithoutSendingAgain(t *testing.T) {
	attempts := 0
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		attempts++
		return notifyResponse(req, http.StatusServiceUnavailable), nil
	})
	options.G.Webhook.MsgNotifyEventRetryMaxCount = 1
	store.saveErr = errors.New("disk full")
	counts := make(map[int64]int)
	_, pending := w.pushMessages([]wkdb.Message{notifyTestMessage(34)}, counts)
	if len(pending) != 1 {
		t.Fatal("归档未成功不能移出缓存")
	}
	store.saveErr = nil
	ok, pending := w.pushMessages(pending, counts)
	exists, _ := store.Has(34)
	if ok || len(pending) != 0 || !exists || attempts != 1 {
		t.Fatal("归档恢复不能绕过已经耗尽的重试次数")
	}
}

func TestAgentArchiveFailureBlocksNormalRetryOnNextRound(t *testing.T) {
	requests := make(map[string]int)
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		requests[req.URL.Host]++
		return notifyResponse(req, http.StatusServiceUnavailable), nil
	})
	options.G.Agent.Webhook.HTTPAddr = "http://agent.invalid/webhook"
	store.saveErrID = map[int64]error{52: errors.New("agent record sync failed")}
	agent := notifyTestMessage(52)
	agent.ChannelType = wkproto.ChannelTypeAgent
	counts := make(map[int64]int)
	_, pending := w.pushMessages([]wkdb.Message{notifyTestMessage(51), agent}, counts)
	_, pending = w.pushMessages(pending, counts)
	if len(pending) != 2 || requests["notify.invalid"] != 1 || requests["agent.invalid"] != 1 {
		t.Fatalf("Agent 归档失败后重发了其他子批：%v", requests)
	}
	delete(store.saveErrID, 52)
	w.pushMessages(pending, counts)
	if requests["notify.invalid"] != 2 || requests["agent.invalid"] != 2 {
		t.Fatalf("归档恢复后的有限重试次数不正确：%v", requests)
	}
}

func TestArchiveLookupFailureDoesNotSend(t *testing.T) {
	attempts := 0
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		attempts++
		return notifyResponse(req, http.StatusOK), nil
	})
	store.hasErr = errors.New("archive unreadable")
	ok, pending := w.pushMessages([]wkdb.Message{notifyTestMessage(33)}, make(map[int64]int))
	if ok || attempts != 0 || len(pending) != 1 {
		t.Fatal("失败记录无法读取时不得自动补发或丢弃缓存")
	}
}

type notifyQueueStub struct {
	data   chan []byte
	closed atomic.Bool
}

func (q *notifyQueueStub) ReadChan() <-chan []byte { return q.data }
func (q *notifyQueueStub) Close() error            { q.closed.Store(true); return nil }
func (q *notifyQueueStub) Put(data []byte) error   { q.data <- data; return nil }
func (q *notifyQueueStub) Delete() error           { return nil }
func (q *notifyQueueStub) Depth() int64            { return int64(len(q.data)) }
func (q *notifyQueueStub) Empty() error            { return nil }

func startNotifyTestLoop(w *Webhook) {
	w.workers.Add(1)
	go func() { defer w.workers.Done(); w.notifyQueueLoop() }()
}

func TestNotifyLoopBackpressureRetainsCacheAndStops(t *testing.T) {
	var attempts atomic.Int32
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return notifyResponse(req, http.StatusServiceUnavailable), nil
	})
	store.saveErr = errors.New("disk full")
	store.saved = make(chan struct{}, 1)
	queue := &notifyQueueStub{data: make(chan []byte, 20)}
	w.backend = queue
	for id := int64(1); id <= 20; id++ {
		message := notifyTestMessage(id)
		data, err := message.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		queue.data <- data
	}
	startNotifyTestLoop(w)
	select {
	case <-store.saved:
	case <-time.After(time.Second):
		t.Fatal("未进入失败保留路径")
	}
	if queue.Depth() != 18 {
		t.Fatalf("未保留失败批次并背压：剩余队列 %d", queue.Depth())
	}
	if err := w.Stop(); err == nil {
		t.Fatal("停止时保留失败必须返回错误")
	}
	if !queue.closed.Load() || attempts.Load() != 1 {
		t.Fatal("停止/重试顺序不正确")
	}
}

func TestStopCancelsInFlightAndRetainsUncertainMessages(t *testing.T) {
	entered := make(chan struct{})
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		close(entered)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	queue := &notifyQueueStub{data: make(chan []byte, 2)}
	w.backend = queue
	store.queue = queue
	for _, id := range []int64{21, 22} {
		message := notifyTestMessage(id)
		data, err := message.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		queue.data <- data
	}
	startNotifyTestLoop(w)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("发送未开始")
	}
	done := make(chan error, 1)
	go func() { done <- w.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("停止未取消请求或等待了额外重发")
	}
	for _, id := range []int64{21, 22} {
		if exists, _ := store.Has(id); !exists {
			t.Fatalf("停止后未保留结果不确定的消息 %d", id)
		}
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestStopRetainsUnsentPartialBatch(t *testing.T) {
	var attempts atomic.Int32
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return notifyResponse(req, http.StatusOK), nil
	})
	options.G.Webhook.MsgNotifyEventPushInterval = time.Hour
	queue := &notifyQueueStub{data: make(chan []byte, 1)}
	w.backend, store.queue = queue, queue
	message := notifyTestMessage(41)
	data, err := message.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	queue.data <- data
	startNotifyTestLoop(w)
	deadline := time.Now().Add(time.Second)
	for queue.Depth() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("队列未被读取")
		}
		time.Sleep(time.Millisecond)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if exists, _ := store.Has(41); !exists || attempts.Load() != 0 {
		t.Fatal("停止应保留不足一批的缓存，不额外发送")
	}
}

func TestStopExitsIdleOnlineStatusLoop(t *testing.T) {
	w, _ := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		return notifyResponse(req, http.StatusOK), nil
	})
	w.backend = &notifyQueueStub{data: make(chan []byte)}
	w.workers.Add(1)
	go func() { defer w.workers.Done(); w.loopOnlineStatus() }()
	done := make(chan error, 1)
	go func() { done <- w.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("空在线状态协程未及时停止")
	}
}
