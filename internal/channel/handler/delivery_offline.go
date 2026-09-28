package handler

import (
	"sync"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// offlineDeliveryBatch 按原消息批次聚合已确认离线的接收者，不等待仍在恢复的用户。
type offlineDeliveryBatch struct {
	mu        sync.Mutex
	events    []*eventbus.Event
	size      uint64
	seen      map[string]struct{}
	pending   []string
	scheduled bool
}

func newOfflineDeliveryBatch(events []*eventbus.Event) *offlineDeliveryBatch {
	batch := &offlineDeliveryBatch{
		events: make([]*eventbus.Event, 0, len(events)),
		seen:   make(map[string]struct{}),
	}
	for _, event := range events {
		if event == nil || event.Conn == nil || event.Frame == nil {
			continue
		}
		if event.Frame.GetFrameType() == wkproto.EVENT {
			continue
		}
		batch.events = append(batch.events, event.Clone())
		batch.size += event.Size()
	}
	return batch
}

func (b *offlineDeliveryBatch) add(uid string) {
	if uid == "" || len(b.events) == 0 {
		return
	}
	b.mu.Lock()
	if _, exists := b.seen[uid]; exists {
		b.mu.Unlock()
		return
	}
	b.seen[uid] = struct{}{}
	b.pending = append(b.pending, uid)
	if b.scheduled {
		b.mu.Unlock()
		return
	}
	b.scheduled = true
	b.mu.Unlock()
	// 同一批次最多保留一个尚未提取的 token，不主动唤醒以复用既有出队节奏。
	eventbus.Pusher.AddEvents([]*eventbus.Event{{
		Type: eventbus.EventPushOffline, TakeOfflineEvents: b.take, OfflineBatchSize: b.size,
	}})
}

func (b *offlineDeliveryBatch) take() []*eventbus.Event {
	b.mu.Lock()
	uids := b.pending
	b.pending = nil
	// 取走新增用户和允许下一次排队必须在同一把锁内，避免并发新增丢失唤醒。
	b.scheduled = false
	b.mu.Unlock()
	if len(uids) == 0 {
		return nil
	}
	events := make([]*eventbus.Event, 0, len(b.events))
	for _, event := range b.events {
		users := make([]string, 0, len(uids))
		for _, uid := range uids {
			if uid != event.Conn.Uid {
				users = append(users, uid)
			}
		}
		if len(users) == 0 {
			continue
		}
		push := event.Clone()
		push.Type, push.OfflineUsers = eventbus.EventPushOffline, users
		push.TakeOfflineEvents = nil
		events = append(events, push)
	}
	return events
}
