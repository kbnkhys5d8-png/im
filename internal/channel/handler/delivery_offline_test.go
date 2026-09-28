package handler

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestOfflineDeliveryBatchFiveThousandConcurrentUIDsShareToken(t *testing.T) {
	pusher := setupOfflineBatchPusher(t)
	first := offlineBatchEvent(1, "sender")
	second := offlineBatchEvent(2, "sender")
	batch := newOfflineDeliveryBatch([]*eventbus.Event{first, second})
	var producers sync.WaitGroup
	for i := range 5000 {
		producers.Add(1)
		go func() {
			defer producers.Done()
			uid := fmt.Sprintf("uid-%d", i)
			batch.add(uid)
			batch.add(uid)
		}()
	}
	producers.Wait()
	if len(pusher.tokens) != 1 || pusher.advances.Load() != 0 {
		t.Fatalf("出队前只允许一个 token 且不能 Advance：tokens=%d advance=%d", len(pusher.tokens), pusher.advances.Load())
	}
	token := <-pusher.tokens
	if token.Type != eventbus.EventPushOffline || token.TakeOfflineEvents == nil {
		t.Fatal("离线批次必须通过本地聚合 token 出队")
	}
	events := token.TakeOfflineEvents()
	if len(events) != 2 {
		t.Fatalf("每条原消息只生成一个用户集合，实际消息数=%d", len(events))
	}
	for i, event := range events {
		seen := make(map[string]bool)
		for _, uid := range event.OfflineUsers {
			if seen[uid] {
				t.Fatalf("用户重复：%s", uid)
			}
			seen[uid] = true
		}
		if event.MessageId != int64(i+1) || len(seen) != 5000 {
			t.Fatalf("聚合结果不完整：message=%d users=%d", event.MessageId, len(seen))
		}
	}
	if len(token.TakeOfflineEvents()) != 0 {
		t.Fatal("重复提取已清空 token 不能重发")
	}
}

func TestOfflineDeliveryBatchFiltersPerMessageAndPreservesTemplate(t *testing.T) {
	pusher := setupOfflineBatchPusher(t)
	first := offlineBatchEvent(1, "first-sender")
	first.OfflineUsers = []string{"original"}
	conn := &eventbus.Conn{Uid: "original", SessionId: "original-session"}
	first.ToConns = []*eventbus.Conn{conn}
	first.ToUid = "original-to"
	second := offlineBatchEvent(2, "second-sender")
	missingConn, missingFrame := offlineBatchEvent(3, "sender"), offlineBatchEvent(4, "sender")
	missingConn.Conn, missingFrame.Frame = nil, nil
	eventOnly := offlineBatchEvent(5, "sender")
	eventOnly.Frame = &wkproto.EventPacket{}
	batch := newOfflineDeliveryBatch([]*eventbus.Event{nil, first, missingConn, second, missingFrame, eventOnly})
	for _, uid := range []string{"", "first-sender", "second-sender", "receiver", "receiver"} {
		batch.add(uid)
	}
	if len(pusher.tokens) != 1 {
		t.Fatalf("应合并成一个 token，实际=%d", len(pusher.tokens))
	}
	events := (<-pusher.tokens).TakeOfflineEvents()
	if len(events) != 2 {
		t.Fatalf("nil、缺字段和 EVENT 不应形成离线消息，实际=%d", len(events))
	}
	wantUsers := [][]string{{"second-sender", "receiver"}, {"first-sender", "receiver"}}
	for i, event := range events {
		if event.Type != eventbus.EventPushOffline || !reflect.DeepEqual(event.OfflineUsers, wantUsers[i]) {
			t.Fatalf("消息%d离线用户错误：%v", i, event.OfflineUsers)
		}
	}
	if !reflect.DeepEqual(first.OfflineUsers, []string{"original"}) || first.ToUid != "original-to" ||
		len(first.ToConns) != 1 || first.ToConns[0] != conn || first.Type != eventbus.EventChannelDistribute {
		t.Fatal("提取离线用户不得污染共享消息模板")
	}
	events[0].OfflineUsers[0] = "changed"
	if events[1].OfflineUsers[0] != "first-sender" || first.OfflineUsers[0] != "original" {
		t.Fatal("不同消息和原模板不能共享可变离线用户切片")
	}
	batch.add("receiver")
	if len(pusher.tokens) != 0 {
		t.Fatal("已输出的同一用户不能再次产生 token")
	}
	batch.add("later")
	if len(pusher.tokens) != 1 {
		t.Fatal("提取后新增用户必须重新排队")
	}
	for _, event := range (<-pusher.tokens).TakeOfflineEvents() {
		if !reflect.DeepEqual(event.OfflineUsers, []string{"later"}) {
			t.Fatal("新波次只能输出尚未提取的用户")
		}
	}
}

func TestOfflineDeliveryBatchConcurrentFlushNeverLosesOrRepeatsUIDs(t *testing.T) {
	pusher := setupOfflineBatchPusher(t)
	batch := newOfflineDeliveryBatch([]*eventbus.Event{offlineBatchEvent(1, "sender")})
	const count = 1000
	seen := make(map[string]int)
	consume := func(token *eventbus.Event) {
		for _, event := range token.TakeOfflineEvents() {
			for _, uid := range event.OfflineUsers {
				seen[uid]++
			}
		}
	}
	done := make(chan struct{})
	var consumer sync.WaitGroup
	consumer.Add(1)
	go func() {
		defer consumer.Done()
		for {
			select {
			case token := <-pusher.tokens:
				consume(token)
			case <-done:
				for {
					select {
					case token := <-pusher.tokens:
						consume(token)
					default:
						return
					}
				}
			}
		}
	}()
	var producers sync.WaitGroup
	for i := range count {
		producers.Add(1)
		go func() {
			defer producers.Done()
			uid := fmt.Sprintf("uid-%d", i)
			batch.add(uid)
			batch.add(uid)
		}()
	}
	producers.Wait()
	close(done)
	consumer.Wait()
	if len(seen) != count {
		t.Fatalf("flush 并发新增丢失用户：got=%d want=%d", len(seen), count)
	}
	for uid, n := range seen {
		if n != 1 {
			t.Fatalf("flush 并发新增重复用户：%s=%d", uid, n)
		}
	}
}

func TestOfflineDeliveryBatchTokenKeepsOriginalQueueBudget(t *testing.T) {
	pusher := setupOfflineBatchPusher(t)
	original := []*eventbus.Event{offlineBatchEvent(1, "sender"), offlineBatchEvent(2, "sender")}
	var budget uint64
	for _, event := range original {
		event.TagKey = strings.Repeat("tag", 512)
		budget += event.Size()
	}
	newOfflineDeliveryBatch(original).add("first")
	newOfflineDeliveryBatch(original).add("second")
	queue := eventbus.NewEventQueue("offline-budget-test")
	queue.Append(<-pusher.tokens)
	queue.Append(<-pusher.tokens)
	// 每个 token 对应一个完整原批次；单批预算不能把两批空壳同时提取。
	if got := queue.SliceWithSize(1, 3, budget); len(got) != 1 {
		t.Fatalf("离线聚合绕过原批次预算：提取 token=%d，预算=%d", len(got), budget)
	}
}

func offlineBatchEvent(id int64, sender string) *eventbus.Event {
	return &eventbus.Event{
		Type: eventbus.EventChannelDistribute, MessageId: id,
		Conn: &eventbus.Conn{Uid: sender}, Frame: &wkproto.SendPacket{},
	}
}

type offlineBatchPusher struct {
	tokens   chan *eventbus.Event
	advances atomic.Int32
}

func setupOfflineBatchPusher(t *testing.T) *offlineBatchPusher {
	t.Helper()
	old := eventbus.Pusher
	t.Cleanup(func() { eventbus.Pusher = old })
	pusher := &offlineBatchPusher{tokens: make(chan *eventbus.Event, 8192)}
	eventbus.RegisterPusher(pusher)
	return pusher
}

func (p *offlineBatchPusher) AddEvent(_ int, event *eventbus.Event) { p.tokens <- event }
func (p *offlineBatchPusher) RandHandlerId() int                    { return 1 }
func (p *offlineBatchPusher) Advance(int)                           { p.advances.Add(1) }
