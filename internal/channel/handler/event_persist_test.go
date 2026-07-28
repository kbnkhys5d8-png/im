package handler

import (
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestHandlerToPersistMessagesMarksOnlyAcceptedPersistentMessagesForSearchOutbox(t *testing.T) {
	handler := &Handler{}
	accepted := testPersistEvent("accepted", wkproto.ReasonSuccess, false)
	noPersist := testPersistEvent("ephemeral", wkproto.ReasonSuccess, true)
	rejected := testPersistEvent("rejected", wkproto.ReasonSystemError, false)

	got := handler.toPersistMessages("channel", 2, []*eventbus.Event{
		accepted, noPersist, rejected,
	})

	if len(got) != 1 || got[0].ClientMsgNo != "accepted" || !got[0].SearchOutbox {
		t.Fatalf("persisted messages = %+v", got)
	}
}

func TestHandlerToPersistMessagesOnlyMarksValidSearchOutboxIdentities(t *testing.T) {
	tests := []struct {
		name        string
		channelID   string
		channelType uint8
		payload     []byte
		zeroMessage bool
		wantOutbox  bool
	}{
		{name: "empty payload", channelID: "channel", channelType: 2},
		{name: "empty channel", channelType: 2, payload: []byte("message")},
		{name: "oversized channel", channelID: strings.Repeat("x", key.MaxSearchOutboxChannelIDBytes+1), channelType: 2, payload: []byte("message")},
		{name: "invalid UTF-8 channel", channelID: string([]byte{0xff}), channelType: 2, payload: []byte("message")},
		{name: "zero channel type", channelID: "channel", payload: []byte("message")},
		{name: "zero message ID", channelID: "channel", channelType: 2, payload: []byte("message"), zeroMessage: true},
		{name: "maximum channel boundary", channelID: strings.Repeat("x", key.MaxSearchOutboxChannelIDBytes), channelType: 2, payload: []byte("message"), wantOutbox: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := &Handler{}
			event := testPersistEvent(test.name, wkproto.ReasonSuccess, false)
			event.Frame.(*wkproto.SendPacket).Payload = test.payload
			if test.zeroMessage {
				event.MessageId = 0
			}

			got := handler.toPersistMessages(test.channelID, test.channelType, []*eventbus.Event{event})

			if len(got) != 1 {
				t.Fatalf("persisted messages = %+v, want one retained message", got)
			}
			if got[0].SearchOutbox != test.wantOutbox {
				t.Fatalf("SearchOutbox = %v, want %v for %+v", got[0].SearchOutbox, test.wantOutbox, got[0])
			}
		})
	}
}

func testPersistEvent(clientMsgNo string, reasonCode wkproto.ReasonCode, noPersist bool) *eventbus.Event {
	return &eventbus.Event{
		Conn: &eventbus.Conn{Uid: "sender"},
		Frame: &wkproto.SendPacket{
			Framer:      wkproto.Framer{NoPersist: noPersist},
			ClientMsgNo: clientMsgNo,
			Payload:     []byte(clientMsgNo),
		},
		MessageId:  1,
		ReasonCode: reasonCode,
	}
}
