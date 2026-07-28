package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
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
