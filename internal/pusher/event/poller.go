package event

import (
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/lni/goutils/syncutil"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"
)

type poller struct {
	handlers []*pushHandler
	wklog.Log
	sync.RWMutex

	stopper    *syncutil.Stopper
	handlePool *ants.Pool
	index      int

	eventPool *EventPool

	contextPool *sync.Pool
	advanceC    chan struct{}
}

func newPoller(index int, eventPool *EventPool) *poller {

	p := &poller{
		stopper:   syncutil.NewStopper(),
		index:     index,
		eventPool: eventPool,
		Log:       wklog.NewWKLog(fmt.Sprintf("poller[%d]", index)),
		contextPool: &sync.Pool{
			New: func() interface{} {
				return &eventbus.PushContext{}
			},
		},
		advanceC: make(chan struct{}, 1),
	}

	var err error
	// The poller executes tasks inline when the bounded pool is full. This keeps
	// ordering and applies backpressure without dropping dequeued events.
	p.handlePool, err = ants.NewPool(options.G.Poller.PushGoroutine, ants.WithNonblocking(true), ants.WithPanicHandler(func(i interface{}) {
		p.Panic("channel handle panic", zap.Any("panic", i), zap.Stack("stack"))
	}))
	if err != nil {
		p.Panic("new ants pool failed", zap.String("error", err.Error()))
	}
	return p
}

func (p *poller) start() error {
	p.stopper.RunWorker(p.loopEvent)
	return nil
}

func (p *poller) stop() {
	p.stopper.Stop()
}

func (p *poller) loopEvent() {
	tk := time.NewTicker(options.G.Poller.IntervalTick)
	defer tk.Stop()
	for {
		p.handleEvents()
		select {
		case <-tk.C:
		case <-p.advanceC:
		case <-p.stopper.ShouldStop():
			return
		}
	}
}

func (p *poller) handleEvents() {
	for _, h := range p.handlers {
		if !h.hasEvent() || !h.tryBeginProcessing() {
			continue
		}
		events := h.events()
		if len(events) == 0 {
			h.finishProcessing()
			continue
		}
		handler := h
		p.submitTask(func() {
			handler.advanceEvents(events)
		})
	}
}

func (p *poller) submitTask(task func()) {
	if err := p.handlePool.Submit(task); err != nil {
		p.runTaskInline(task)
	}
}

func (p *poller) runTaskInline(task func()) {
	defer func() {
		if v := recover(); v != nil {
			p.Error("push inline task panic", zap.Any("panic", v), zap.Stack("stack"))
		}
	}()
	task()
}

func (p *poller) advance() {
	select {
	case p.advanceC <- struct{}{}:
	default:
	}
}

func (p *poller) getContext() *eventbus.PushContext {
	return p.contextPool.Get().(*eventbus.PushContext)
}

func (p *poller) putContext(ctx *eventbus.PushContext) {
	ctx.Reset()
	p.contextPool.Put(ctx)
}
