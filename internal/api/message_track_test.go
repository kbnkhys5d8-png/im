package api

import (
	"bytes"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/internal/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/spf13/viper"
)

func TestSendMessageToChannelInitializesTrackBeforeEnqueue(t *testing.T) {
	previousOptions, previousChannel := options.G, eventbus.Channel
	t.Cleanup(func() { options.G, eventbus.Channel = previousOptions, previousChannel })
	options.G = options.New()
	config := viper.New()
	config.Set("rootDir", t.TempDir())
	config.Set("jwt.secret", "unit-test-only")
	options.G.ConfigureWithViper(config)

	channel := &messageTrackChannel{}
	eventbus.RegisterChannel(channel)
	payload := []byte(`{"type":1,"content":"消息轨迹测试"}`)
	req := messageSendReq{
		FromUID: "sender", Payload: payload, TagKey: "routing-tag", Expire: 60,
		Header: types.MessageHeader{RedDot: 1, NoPersist: 0, SyncOnce: 0},
	}
	messageID, err := sendMessageToChannel(req, "group", wkproto.ChannelTypeGroup, "client-message")
	if err != nil {
		t.Fatal(err)
	}
	if channel.event == nil || channel.added != 1 || channel.advanced != 1 {
		t.Fatalf("应只入队并推进一次：入队=%d 推进=%d", channel.added, channel.advanced)
	}
	startFlag := uint16(1 << (16 - track.PositionStart))
	if channel.beforeConsumer.Path != startFlag || channel.beforeConsumer.PreStart.IsZero() {
		t.Errorf("开始轨迹必须在入队前完成，实际路径=%d", channel.beforeConsumer.Path)
	}
	if channel.event.Track != channel.afterConsumer {
		t.Error("入队后生产者不得改写消费方已接管的轨迹")
	}
	if channel.channelID != "group" || channel.channelType != wkproto.ChannelTypeGroup ||
		channel.event.MessageId != messageID || messageID <= 0 || channel.event.TagKey != req.TagKey {
		t.Fatal("入队频道、消息编号或路由标签被改变")
	}
	if channel.event.Conn == nil || !channel.event.Conn.Internal || channel.event.Conn.Uid != req.FromUID {
		t.Fatal("内部发送身份被改变")
	}
	frame, ok := channel.event.Frame.(*wkproto.SendPacket)
	if !ok || frame.ChannelID != "group" || frame.ChannelType != wkproto.ChannelTypeGroup ||
		frame.ClientMsgNo != "client-message" || frame.Expire != req.Expire ||
		!frame.RedDot || frame.NoPersist || frame.SyncOnce || !bytes.Equal(frame.Payload, payload) {
		t.Fatal("消息内容、持久化或协议字段被改变")
	}
}

type messageTrackChannel struct {
	event          *eventbus.Event
	channelID      string
	channelType    uint8
	beforeConsumer track.Message
	afterConsumer  track.Message
	added          int
	advanced       int
}

func (c *messageTrackChannel) AddEvent(channelID string, channelType uint8, event *eventbus.Event) {
	c.added++
	c.event, c.channelID, c.channelType = event, channelID, channelType
	c.beforeConsumer = event.Track.Clone()
	// 入队即模拟消费方接手，确定性检查生产者没有在交接后继续写轨迹。
	event.Track.Record(track.PositionChannelOnSend)
	c.afterConsumer = event.Track.Clone()
}

func (c *messageTrackChannel) Advance(channelID string, channelType uint8) {
	if c.channelID == channelID && c.channelType == channelType {
		c.advanced++
	}
}
