package event

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

type pushHandler struct {
	id int
	wklog.Log
	pending struct {
		sync.RWMutex
		eventQueue *eventbus.EventQueue
	}
	poller  *poller
	handler eventbus.PushEventHandler
	// 处理中的下标位置
	processingIndex uint64
	processing      atomic.Bool
}

func newPushHandler(id int, poller *poller) *pushHandler {

	uh := &pushHandler{
		id:      id,
		poller:  poller,
		handler: poller.eventPool.handler,
		Log:     wklog.NewWKLog(fmt.Sprintf("pushHandler[%d]", id)),
	}
	uh.pending.eventQueue = eventbus.NewEventQueue(fmt.Sprintf("push:%d", id))
	return uh
}

func (p *pushHandler) addEvent(event *eventbus.Event) {
	p.pending.Lock()
	defer p.pending.Unlock()
	event.Index = p.pending.eventQueue.LastIndex() + 1
	p.pending.eventQueue.Append(event)

}

func (p *pushHandler) hasEvent() bool {
	p.pending.RLock()
	defer p.pending.RUnlock()
	if p.processing.Load() {
		return false
	}
	return p.processingIndex < p.pending.eventQueue.LastIndex()
}

func (p *pushHandler) tryBeginProcessing() bool {
	return p.processing.CompareAndSwap(false, true)
}

func (p *pushHandler) finishProcessing() {
	p.processing.Store(false)
}

func (p *pushHandler) events() []*eventbus.Event {
	p.pending.Lock()
	defer p.pending.Unlock()
	events := p.pending.eventQueue.SliceWithSize(p.processingIndex+1, p.pending.eventQueue.LastIndex()+1, options.G.Poller.PushEventMaxSizePerBatch)
	if len(events) == 0 {
		return nil
	}
	eventLastIndex := events[len(events)-1].Index

	// 截取掉之前的事件
	p.pending.eventQueue.TruncateTo(eventLastIndex + 1)
	p.processingIndex = eventLastIndex
	return events
}

// 推进事件
func (p *pushHandler) advanceEvents(events []*eventbus.Event) {
	defer func() {
		p.finishProcessing()
		if p.hasEvent() {
			p.poller.advance()
		}
	}()

	// 按类型分组
	group := p.groupByType(events)
	// 处理事件
	for eventType, events := range group {
		// 从对象池中获取上下文
		ctx := p.poller.getContext()
		ctx.Id = p.id
		ctx.EventType = eventType
		ctx.Events = events
		// 处理事件
		p.handler.OnEvent(ctx)

		// 释放上下文
		p.poller.putContext(ctx)
	}

}

// groupByType 将待处理事件按照事件类型分组
func (p *pushHandler) groupByType(events []*eventbus.Event) map[eventbus.EventType][]*eventbus.Event {
	group := make(map[eventbus.EventType][]*eventbus.Event)
	for _, event := range events {
		group[event.Type] = append(group[event.Type], event)
	}
	return group
}
