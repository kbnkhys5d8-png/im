package api

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func (s *historyAPISlot) ProposeUntilApplied(_ uint32, data []byte) (*rafttypes.ProposeResp, error) {
	return &rafttypes.ProposeResp{}, s.store.ApplySlotLogs(0, []rafttypes.Log{{Index: 1, Data: data}})
}

type groupPurgeTestPlugins struct{ service.IPluginManager }

func (groupPurgeTestPlugins) Plugins(...types.PluginMethod) []types.Plugin { return nil }

func TestGroupPurgeLocalEndpointPreflightAndDelete(t *testing.T) {
	r, db, _ := historyAPIFixture(t)
	old := service.PluginManager
	service.PluginManager = groupPurgeTestPlugins{}
	t.Cleanup(func() { service.PluginManager = old })
	t.Setenv("WK_GROUP_PURGE_NO_EXTERNAL_SEARCH", "")
	t.Setenv("SEARCH_OPENSEARCH_URL", "")
	const channel = "synthetic-group"
	const kind = wkproto.ChannelTypeGroup
	original := wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: channel, ChannelType: kind,
		MessageID: 9001, MessageSeq: 1, FromUID: "synthetic", Payload: []byte("synthetic")}}
	require.NoError(t, db.AppendMessages(channel, kind, []wkdb.Message{original}))
	_, err := db.AddChannel(wkdb.ChannelInfo{ChannelId: channel, ChannelType: kind})
	require.NoError(t, err)
	require.NoError(t, db.AddSubscribers(channel, kind, []wkdb.Member{{Uid: "member"}}))
	req := map[string]string{"group_no": channel}
	require.Equal(t, 403, historyHTTP(r, "/history/group-purge", req, "192.0.2.1:9010", "synthetic-history-token").Code)
	require.Equal(t, 409, historyLocalHTTP(r, "/history/group-purge", req).Code)
	t.Setenv("WK_GROUP_PURGE_NO_EXTERNAL_SEARCH", "verified")
	options.G.Plugin.Install = []string{"synthetic-plugin"}
	require.Equal(t, 409, historyLocalHTTP(r, "/history/group-purge", req).Code)
	options.G.Plugin.Install = nil
	require.Equal(t, 200, historyLocalHTTP(r, "/history/group-purge/check", req).Code)
	_, err = db.LoadMsg(channel, kind, 1)
	require.NoError(t, err)
	out := historyLocalHTTP(r, "/history/group-purge", req)
	require.Equal(t, 200, out.Code, out.Body.String())
	require.Equal(t, 200, historyLocalHTTP(r, "/history/group-purge", req).Code)
	_, err = db.LoadMsg(channel, kind, 1)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	members, err := db.GetSubscribers(channel, kind)
	require.NoError(t, err)
	require.Empty(t, members)
	info, err := db.GetChannel(channel, kind)
	require.NoError(t, err)
	require.True(t, info.Disband)
	require.NoError(t, db.AddOrUpdateConversations([]wkdb.Conversation{
		{Uid: "member", ChannelId: channel, ChannelType: kind, Id: 501},
		{Uid: "member", ChannelId: "synthetic-other", ChannelType: kind, Id: 502},
	}))
	removed, err := db.GetConversation("member", channel, kind)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	require.True(t, wkdb.IsEmptyConversation(removed))
	survivor, err := db.GetConversation("member", "synthetic-other", kind)
	require.NoError(t, err)
	require.False(t, wkdb.IsEmptyConversation(survivor))

	otherGroup := "synthetic-other"
	commandID := options.G.OrginalConvertCmdChannel(otherGroup)
	original.MessageID, original.ChannelID = 9002, commandID
	require.NoError(t, db.AppendMessages(commandID, kind, []wkdb.Message{original}))
	require.Equal(t, 409, historyLocalHTTP(r, "/history/group-purge", map[string]string{"group_no": otherGroup}).Code)
	_, err = db.LoadMsg(commandID, kind, 1)
	require.NoError(t, err)
}
