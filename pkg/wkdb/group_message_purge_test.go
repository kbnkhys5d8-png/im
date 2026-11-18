package wkdb_test

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

type purgeGroupDB interface {
	CheckGroupMessagePurge(context.Context, string) error
	PurgeGroupMessages(context.Context, string) error
	AppendMessagesForRaft(string, uint8, []wkdb.Message) error
}

func purgeFixtureMessage(channel string, kind uint8, id int64, stream string) wkdb.Message {
	return wkdb.Message{RecvPacket: wkproto.RecvPacket{
		ChannelID: channel, ChannelType: kind, MessageID: id, MessageSeq: 1,
		FromUID: "sender", ClientMsgNo: "synthetic", StreamNo: stream, Timestamp: 100,
		Payload: []byte("synthetic message"),
	}, SearchOutbox: true}
}

func TestPurgeGroupMessagesIsScopedAndDurable(t *testing.T) {
	path := t.TempDir()
	openDB := func() wkdb.DB {
		db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(path), wkdb.WithShardNum(1)))
		require.NoError(t, db.Open())
		return db
	}
	db := openDB()
	t.Cleanup(func() {
		if db != nil {
			require.NoError(t, db.Close())
		}
	})
	const group = wkproto.ChannelTypeGroup
	const person = wkproto.ChannelTypePerson
	for _, m := range []wkdb.Message{
		purgeFixtureMessage("target", group, 10, ""),
		purgeFixtureMessage("neighbor", group, 11, ""),
		purgeFixtureMessage("target", person, 12, ""),
	} {
		require.NoError(t, db.AppendMessages(m.ChannelID, m.ChannelType, []wkdb.Message{m}))
	}
	purge := db.(purgeGroupDB)
	require.NoError(t, purge.PurgeGroupMessages(context.Background(), "target"))
	require.NoError(t, purge.PurgeGroupMessages(context.Background(), "target"))
	require.NoError(t, db.Close())
	db = nil
	db = openDB()
	purge = db.(purgeGroupDB)
	_, err := db.GetMessage(10)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	_, err = db.LoadMsg("target", group, 1)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	for _, id := range []uint64{11, 12} {
		m, err := db.GetMessage(id)
		require.NoError(t, err)
		require.Equal(t, "synthetic message", string(m.Payload))
	}
	_, enabled, err := db.GetSearchOutboxFloor("target", group)
	require.NoError(t, err)
	require.False(t, enabled)
	next := []wkdb.Message{purgeFixtureMessage("target", group, 13, "")}
	require.ErrorIs(t, db.AppendMessages("target", group, next), wkdb.ErrGroupMessagesPurged)
	require.ErrorIs(t, purge.AppendMessagesForRaft("target", group, next), wkdb.ErrGroupMessagesPurged)
	_, _, err = db.AppendMessageEventWithState(purgeStreamEvent("target"))
	require.ErrorIs(t, err, wkdb.ErrGroupMessagesPurged)
	_, _, err = db.AppendMessageEventWithState(purgeStreamEvent("neighbor"))
	require.NoError(t, err)
}

func TestPurgeGroupMessagesRejectsUnsupportedHistoryWithoutWrites(t *testing.T) {
	const kind = wkproto.ChannelTypeGroup
	for _, tc := range []struct {
		name    string
		stream  string
		setting wkproto.Setting
		orphan  bool
	}{
		{name: "legacy stream number", stream: "stream"},
		{name: "stream setting only", setting: wkproto.SettingStream},
		{name: "unlinked stream snapshot", orphan: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			require.NoError(t, db.Open())
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			original := purgeFixtureMessage("target", kind, 21, "")
			stream := purgeFixtureMessage("target", kind, 22, tc.stream)
			stream.MessageSeq, stream.Setting = 2, tc.setting
			require.NoError(t, db.AppendMessages("target", kind, []wkdb.Message{original, stream}))
			if tc.orphan {
				event := purgeStreamEvent("target")
				event.ClientMsgNo = "missing-original-message"
				_, _, err := db.AppendMessageEventWithState(event)
				require.NoError(t, err)
			}
			purge := db.(purgeGroupDB)
			require.ErrorIs(t, purge.CheckGroupMessagePurge(context.Background(), "target"), wkdb.ErrGroupMessagePurgeUnsupported)
			require.ErrorIs(t, purge.PurgeGroupMessages(context.Background(), "target"), wkdb.ErrGroupMessagePurgeUnsupported)
			for _, id := range []uint64{21, 22} {
				_, err := db.GetMessage(id)
				require.NoError(t, err)
			}
			next := purgeFixtureMessage("target", kind, 23, "")
			next.MessageSeq = 3
			require.NoError(t, db.AppendMessages("target", kind, []wkdb.Message{next}))
		})
	}
}

func purgeStreamEvent(channel string) *wkdb.MessageEvent {
	return &wkdb.MessageEvent{ChannelId: channel, ChannelType: wkproto.ChannelTypeGroup,
		ClientMsgNo: "synthetic", EventID: "stream-event", EventType: wkdb.EventTypeStreamDelta,
		Payload: []byte(`{"kind":"text","delta":"synthetic stream"}`)}
}

func TestPurgeGroupMessagesRejectsIndexPointingToAnotherGroup(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	const kind = wkproto.ChannelTypeGroup
	for _, channel := range []string{"target", "neighbor"} {
		m := purgeFixtureMessage(channel, kind, 31, "")
		require.NoError(t, db.AppendMessages(channel, kind, []wkdb.Message{m}))
	}
	purge := db.(purgeGroupDB)
	require.ErrorIs(t, purge.PurgeGroupMessages(context.Background(), "target"), wkdb.ErrGroupMessagePurgeUnsupported)
	_, err := db.LoadMsg("target", kind, 1)
	require.NoError(t, err)
	m, err := db.GetMessage(31)
	require.NoError(t, err)
	require.Equal(t, "neighbor", m.ChannelID)
}
