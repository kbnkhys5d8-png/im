package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
)

func (h *Handler) pushOffline(ctx *eventbus.PushContext) {
	// 聚合 token 只在本地出队时展开，普通离线事件仍沿用原流程。
	events := make([]*eventbus.Event, 0, len(ctx.Events))
	for _, event := range ctx.Events {
		if event.TakeOfflineEvents == nil {
			events = append(events, event)
			continue
		}
		events = append(events, event.TakeOfflineEvents()...)
	}
	if len(events) == 0 {
		return
	}
	for _, e := range events {

		for _, toUid := range e.OfflineUsers {
			fromUid := e.Conn.Uid
			// 是否是AI
			if fromUid != toUid && h.isAI(toUid) && !e.Frame.GetsyncOnce() && !options.G.IsSystemUid(fromUid) {
				// 处理AI推送
				h.processAIPush(toUid, e)
			}
		}
	}
	service.Webhook.NotifyOfflineMsg(events)
}
