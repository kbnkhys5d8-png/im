package eventbus

import (
	"bytes"
	"testing"
)

func TestOfflineBatchSizeStaysLocalAndSurvivesClone(t *testing.T) {
	event := &Event{
		Type: EventPushOffline, OfflineBatchSize: 8192,
		TakeOfflineEvents: func() []*Event { return nil },
	}
	clone := event.Clone()
	if clone.OfflineBatchSize != event.OfflineBatchSize || clone.Size() != 8192 {
		t.Fatal("离线 token 克隆后必须保留原批次预算")
	}
	encoded, err := (EventBatch{event}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	plain := &Event{Type: EventPushOffline}
	want, err := (EventBatch{plain}).Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, want) {
		t.Fatal("本地原批次预算不能改变节点间编码")
	}
	var decoded EventBatch
	if err := decoded.Decode(encoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 || decoded[0].OfflineBatchSize != 0 || decoded[0].TakeOfflineEvents != nil {
		t.Fatal("节点间解码不能恢复本地预算或提取回调")
	}
}

func TestOfflineBatchSizeDoesNotChangeOrdinaryEventBudget(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event *Event
	}{
		{name: "普通离线消息", event: &Event{Type: EventPushOffline, TagKey: "tag"}},
		{name: "在线消息即使带回调也不使用离线预算", event: &Event{Type: EventPushOnline, TakeOfflineEvents: func() []*Event { return nil }}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.event.Size()
			tc.event.OfflineBatchSize = 8192
			if tc.event.Size() != before {
				t.Fatal("非离线聚合 token 的大小不能受内部预算字段影响")
			}
		})
	}
	base := (&Event{Type: EventPushOffline}).Size()
	token := &Event{Type: EventPushOffline, OfflineBatchSize: 1, TakeOfflineEvents: func() []*Event { return nil }}
	if token.Size() < base {
		t.Fatal("原批次预算不能缩小 token 自身的基础大小")
	}
}
