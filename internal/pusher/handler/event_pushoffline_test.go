package handler

import (
	"bytes"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestPushOfflineExpandsLocalBatchToken(t *testing.T) {
	oldOptions, oldWebhook, oldPlugins := options.G, service.Webhook, service.PluginManager
	t.Cleanup(func() { options.G, service.Webhook, service.PluginManager = oldOptions, oldWebhook, oldPlugins })
	options.G = &options.Options{}
	webhook := &offlineTestWebhook{}
	plugins := &offlineTestPlugins{}
	service.Webhook, service.PluginManager = webhook, plugins
	message := func(id int64, uid string) *eventbus.Event {
		return &eventbus.Event{
			Type: eventbus.EventPushOffline, MessageId: id,
			Conn: &eventbus.Conn{Uid: "sender"}, Frame: &wkproto.SendPacket{},
			OfflineUsers: []string{uid},
		}
	}
	legacy, first, second := message(1, "legacy"), message(2, "first"), message(3, "second")
	var taken int
	token := &eventbus.Event{Type: eventbus.EventPushOffline, TakeOfflineEvents: func() []*eventbus.Event {
		taken++
		return []*eventbus.Event{first, second}
	}}
	ctx := &eventbus.PushContext{Events: []*eventbus.Event{legacy, token}}
	(&Handler{}).pushOffline(ctx)
	if taken != 1 || len(webhook.calls) != 1 || len(webhook.calls[0]) != 3 {
		t.Fatalf("token 提取=%d，回调批次=%v", taken, webhook.calls)
	}
	for i, want := range []*eventbus.Event{legacy, first, second} {
		if webhook.calls[0][i] != want {
			t.Fatal("普通事件和聚合消息必须按原顺序进入既有回调")
		}
	}
	if len(plugins.checked) != 3 {
		t.Fatal("聚合消息必须保留原有离线机器人检查")
	}
	if len(ctx.Events) != 2 || ctx.Events[1] != token {
		t.Fatal("展开 token 不得修改原输入批次")
	}
	(&Handler{}).pushOffline(&eventbus.PushContext{Events: []*eventbus.Event{{
		Type: eventbus.EventPushOffline, TakeOfflineEvents: func() []*eventbus.Event { return nil },
	}}})
	if len(webhook.calls) != 1 {
		t.Fatal("已取空的聚合 token 不应重复触发离线回调")
	}
}

func TestOfflineBatchTokenCloneAndEncoding(t *testing.T) {
	var calls int
	event := &eventbus.Event{Type: eventbus.EventPushOffline, TakeOfflineEvents: func() []*eventbus.Event {
		calls++
		return nil
	}}
	clone := event.Clone()
	if clone.TakeOfflineEvents == nil {
		t.Fatal("Clone 必须保留本地 token 的提取入口")
	}
	clone.TakeOfflineEvents()
	if calls != 1 {
		t.Fatal("Clone 提取的必须是原聚合批次")
	}
	encoded, err := (eventbus.EventBatch{event}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	plain := &eventbus.Event{Type: event.Type}
	want, err := (eventbus.EventBatch{plain}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, want) || event.Size() != plain.Size() {
		t.Fatal("本地回调不能改变节点间事件编码或大小")
	}
	var decoded eventbus.EventBatch
	if err := decoded.Decode(encoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 || decoded[0].TakeOfflineEvents != nil {
		t.Fatal("节点间解码不能恢复本地 token 回调")
	}
}

type offlineTestWebhook struct {
	service.IWebhook
	calls [][]*eventbus.Event
}

func (w *offlineTestWebhook) NotifyOfflineMsg(events []*eventbus.Event) {
	w.calls = append(w.calls, append([]*eventbus.Event(nil), events...))
}

type offlineTestPlugins struct {
	service.IPluginManager
	checked []string
}

func (p *offlineTestPlugins) UserIsAI(uid string) bool {
	p.checked = append(p.checked, uid)
	return false
}
