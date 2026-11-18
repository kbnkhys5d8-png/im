package store

import (
	"context"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestGroupPurgeNonSingleNodeDoesNotMutate(t *testing.T) {
	db := historyTestDB(t, t.TempDir())
	defer func() { require.NoError(t, db.Close()) }()
	s := New(NewOptions(WithDB(db),
		WithHistoryTopology(func(string, uint8) error { return wkdb.ErrHistoryUnsupported }),
		WithHistoryCapabilities(func() error { return wkdb.ErrHistoryUnsupported }),
		WithHistoryDrain(func(context.Context, string, uint8) error {
			t.Fatal("非单节点不能进入清理阶段")
			return nil
		})))
	const channel = "synthetic-group"
	const kind = wkproto.ChannelTypeGroup
	msg := wkdb.Message{RecvPacket: wkproto.RecvPacket{
		ChannelID: channel, ChannelType: kind, MessageID: 51, MessageSeq: 1, Payload: []byte("synthetic"),
	}}
	require.NoError(t, db.AppendMessages(channel, kind, []wkdb.Message{msg}))
	require.ErrorIs(t, s.GroupPurge(context.Background(), channel, channel+"&cmd", false), wkdb.ErrHistoryUnsupported)
	purged, err := s.GroupMessagesPurged(channel)
	require.NoError(t, err)
	require.False(t, purged)
	result, err := db.LoadMsg(channel, kind, 1)
	require.NoError(t, err)
	require.Equal(t, msg.Payload, result.Payload)
}

func TestGroupPurgeStreamEventInCommandChannel(t *testing.T) {
	for _, tc := range []struct {
		name         string
		beforeDelete bool
	}{
		{name: "stream data blocks deletion", beforeDelete: true},
		{name: "late replay is discarded without blocking the slot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := historyTestDB(t, t.TempDir())
			defer func() { require.NoError(t, db.Close()) }()
			slot := &historyTestSlot{}
			s := New(NewOptions(WithDB(db), WithSlot(slot),
				WithHistoryCapabilities(func() error { return nil }),
				WithHistoryTopology(func(string, uint8) error { return nil }),
				WithHistoryDrain(func(context.Context, string, uint8) error { return nil }),
				WithHistorySourceChannel(func(id string) string { return strings.TrimSuffix(id, "&cmd") })))
			slot.apply = func(data []byte) error { return s.ApplySlotLogs(0, []types.Log{{Index: 1, Data: data}}) }

			const channel, commandID = "target", "target&cmd"
			const kind = wkproto.ChannelTypeGroup
			applyEvent := func(id string) {
				event := &wkdb.MessageEvent{ChannelId: id, ChannelType: kind, ClientMsgNo: "stream", EventID: "event",
					EventType: wkdb.EventTypeStreamDelta, Payload: []byte(`{"kind":"text","delta":"synthetic"}`)}
				cmd := NewCMDWithVersion(CMDAppendMessageEvent, EncodeCMDMessageEvent(event, CmdVersionMessageEvent), CmdVersionMessageEvent)
				data, err := cmd.Marshal()
				require.NoError(t, err)
				require.NoError(t, slot.apply(data))
			}
			if tc.beforeDelete {
				applyEvent(commandID)
				require.ErrorIs(t, s.GroupPurge(context.Background(), channel, commandID, true), wkdb.ErrGroupMessagePurgeUnsupported)
				require.ErrorIs(t, s.GroupPurge(context.Background(), channel, commandID, false), wkdb.ErrGroupMessagePurgeUnsupported)
				purged, err := s.GroupMessagesPurged(channel)
				require.NoError(t, err)
				require.False(t, purged)
				return
			}
			require.NoError(t, s.GroupPurge(context.Background(), channel, commandID, false))
			applyEvent(commandID)
			state, err := db.GetMessageEventState(commandID, kind, "stream", "")
			require.NoError(t, err)
			require.Nil(t, state)
			applyEvent("neighbor")
			state, err = db.GetMessageEventState("neighbor", kind, "stream", "")
			require.NoError(t, err)
			require.NotNil(t, state)
		})
	}
}
