package api

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestSendEventInitialDistributionPreservesInternalEvent(t *testing.T) {
	previousOptions, previousChannel, previousStore := options.G, eventbus.Channel, service.Store
	options.G = &options.Options{SystemDeviceId: "internal-event-device"}
	options.G.Channel.OnlineCmdChannelId = "online-command-channel"
	// sendEvent 只排入分发事件，不应访问持久层或启动网络服务。
	service.Store = nil
	t.Cleanup(func() {
		options.G, eventbus.Channel, service.Store = previousOptions, previousChannel, previousStore
	})
	tests := []struct {
		name          string
		fakeChannelID string
		channelType   uint8
		eventSeq      uint64
		wantType      eventbus.EventType
	}{
		{
			name: "普通群事件零序号仍进入首次分发", fakeChannelID: "group-fake-id",
			channelType: wkproto.ChannelTypeGroup, wantType: eventbus.EventChannelDistributeInitial,
		},
		{
			name: "个人频道内部事件进入首次分发", fakeChannelID: "peer@sender",
			channelType: wkproto.ChannelTypePerson, eventSeq: 8, wantType: eventbus.EventChannelDistributeInitial,
		},
		{
			name: "在线命令频道保留既有分发路径", fakeChannelID: options.G.Channel.OnlineCmdChannelId,
			channelType: wkproto.ChannelTypeGroup, eventSeq: 3, wantType: eventbus.EventChannelDistribute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			channel := &textLimitChannel{}
			eventbus.RegisterChannel(channel)
			req := &eventAppendReq{
				ChannelID: "visible-channel-id", ChannelType: tt.channelType, FromUID: "sender",
				MessageID: 17, ClientMsgNo: "event-client-msg", EventID: "event-id",
				EventType: "stream.delta", EventKey: "request-key", Visibility: "public", OccurredAt: 123456,
				Payload: json.RawMessage(`{"text":"追加内容","index":2}`),
			}
			const messageID int64 = 9012
			const eventKey, streamStatus = "effective-key", "open"
			newMessage(nil).sendEvent(req, tt.fakeChannelID, eventKey, messageID, tt.eventSeq, streamStatus)
			if len(channel.events) != 1 {
				t.Fatalf("内部 EVENT 必须直接分发一次，实际入队=%d", len(channel.events))
			}
			event := channel.events[0]
			if event.Type != tt.wantType || event.Type == eventbus.EventChannelOnSend {
				t.Fatalf("分发类型=%v，期望=%v，不可退回发送及持久化流程", event.Type, tt.wantType)
			}
			if event.MessageId != messageID || event.MessageSeq != 0 || event.ReasonCode != 0 {
				t.Fatalf("消息ID或合法零值被改写：id=%d seq=%d reason=%d", event.MessageId, event.MessageSeq, event.ReasonCode)
			}
			if event.Conn == nil || !event.Conn.Internal || event.Conn.Uid != req.FromUID ||
				event.Conn.DeviceId != options.G.SystemDeviceId {
				t.Fatalf("必须保留内部发送连接身份：%+v", event.Conn)
			}
			frame, ok := event.Frame.(*wkproto.EventPacket)
			if !ok || frame.GetFrameType() != wkproto.EVENT {
				t.Fatalf("内部事件不能改造成 SEND 包：%T", event.Frame)
			}
			if frame.Type != req.EventType || frame.Id != req.EventID || frame.Timestamp != req.OccurredAt {
				t.Fatalf("EVENT 包字段改变：%+v", frame)
			}
			var got eventPushData
			if err := json.Unmarshal(frame.Data, &got); err != nil {
				t.Fatal(err)
			}
			want := eventPushData{
				ClientMsgNo: req.ClientMsgNo, ChannelID: req.ChannelID, ChannelType: req.ChannelType,
				FromUID: req.FromUID, MessageID: messageID, EventKey: eventKey, MsgEventSeq: tt.eventSeq,
				StreamStatus: streamStatus, Visibility: req.Visibility, Payload: req.Payload,
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("EVENT 数据合同改变：got=%+v want=%+v", got, want)
			}
		})
	}
}
