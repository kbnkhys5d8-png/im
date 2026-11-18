package api

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
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

type groupSearchTestManager struct {
	service.IPluginManager
	search types.Plugin
}

func (s groupSearchTestManager) Plugins(...types.PluginMethod) []types.Plugin {
	return []types.Plugin{s.search}
}

type groupSearchTestPlugin struct {
	types.Plugin
	executeOK bool
}

func (*groupSearchTestPlugin) GetNo() string              { return "wk.plugin.search" }
func (*groupSearchTestPlugin) Status() types.PluginStatus { return types.PluginStatusNormal }
func (p *groupSearchTestPlugin) Route(_ context.Context, req *pluginproto.HttpRequest) (*pluginproto.HttpResponse, error) {
	check := req.Path == "/internal/group-purge-check"
	if !check && !p.executeOK {
		return &pluginproto.HttpResponse{Status: http.StatusServiceUnavailable}, nil
	}
	var request struct {
		Group string `json:"group_no"`
	}
	if err := json.Unmarshal(req.Body, &request); err != nil {
		return nil, err
	}
	body, err := json.Marshal(map[string]any{"group_no": request.Group, "checked": check, "protocol": 1, "verified_empty": !check})
	return &pluginproto.HttpResponse{Status: http.StatusOK, Body: body}, err
}

func TestGroupPurgeOpenSearchFailureRequiresSameGroupRetry(t *testing.T) {
	r, db, _ := historyAPIFixture(t)
	old := service.PluginManager
	plugin := &groupSearchTestPlugin{}
	service.PluginManager = groupSearchTestManager{search: plugin}
	t.Cleanup(func() { service.PluginManager = old })
	t.Setenv("WK_GROUP_PURGE_OPENSEARCH", "verified")
	t.Setenv("WK_GROUP_PURGE_NO_EXTERNAL_SEARCH", "")
	const group = "synthetic-search"
	const kind = wkproto.ChannelTypeGroup
	require.NoError(t, db.AppendMessages(group, kind, []wkdb.Message{{RecvPacket: wkproto.RecvPacket{
		ChannelID: group, ChannelType: kind, MessageID: 555, MessageSeq: 1, Payload: []byte("synthetic"),
	}}}))
	request := map[string]string{"group_no": group}
	check := historyLocalHTTP(r, "/history/group-purge/check", request)
	require.Equal(t, 200, check.Code, check.Body.String())
	require.Contains(t, check.Body.String(), `"search_enabled":true`)
	result := historyLocalHTTP(r, "/history/group-purge", request)
	require.Equal(t, 503, result.Code, result.Body.String())
	_, err := db.LoadMsg(group, kind, 1)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
	plugin.executeOK = true
	result = historyLocalHTTP(r, "/history/group-purge", request)
	require.Equal(t, 200, result.Code, result.Body.String())
	require.Contains(t, result.Body.String(), `"search_verified":true`)
}

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
