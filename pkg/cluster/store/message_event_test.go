package store

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

type textEventProposalSlot struct {
	icluster.Slot
	proposals int
}

func (s *textEventProposalSlot) GetSlotId(string) uint32 { return 1 }

func (s *textEventProposalSlot) ProposeUntilApplied(uint32, []byte) (*types.ProposeResp, error) {
	s.proposals++
	return &types.ProposeResp{Index: 1}, nil
}

type textEventStateDB struct{ wkdb.DB }

func (textEventStateDB) GetMessageEventState(channelID string, channelType uint8, clientMsgNo, eventKey string) (*wkdb.MessageEventState, error) {
	return &wkdb.MessageEventState{
		ChannelId: channelID, ChannelType: channelType, ClientMsgNo: clientMsgNo,
		EventKey: eventKey, LastMsgEventSeq: 1,
	}, nil
}

func TestAppendMessageEventRejectsOverlongCompleteTextBeforeRaftProposal(t *testing.T) {
	for _, eventType := range []string{wkdb.EventTypeStreamSnapshot, wkdb.EventTypeStreamClose} {
		t.Run(eventType, func(t *testing.T) {
			slot := &textEventProposalSlot{}
			store := New(NewOptions(WithSlot(slot), WithDB(textEventStateDB{})))
			body := map[string]any{"kind": "text", "text": strings.Repeat("😀", 5001)}
			if eventType == wkdb.EventTypeStreamClose {
				body = map[string]any{"snapshot": body}
			}
			payload, _ := json.Marshal(body)
			_, _, err := store.AppendMessageEventWithState("group", 2, &wkdb.MessageEvent{
				ClientMsgNo: "client", EventID: "event", EventType: eventType, Payload: payload,
			})
			if err == nil {
				t.Fatal("完整正文超限必须在提案前拒绝")
			}
			if slot.proposals != 0 {
				t.Fatalf("拒绝事件仍发起 %d 次 Raft 提案", slot.proposals)
			}
		})
	}
}
