//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func startConnectionFollowupTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	httpAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort(t))
	clusterAddr := fmt.Sprintf("tcp://127.0.0.1:%d", getFreePort(t))
	// 缩短 Unix socket 路径；该目录不属于任何已运行的 IM。
	socketDir, err := os.MkdirTemp("/tmp", "wknet-registry-")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	s := NewTestServer(t,
		options.WithMode(options.TestMode),
		options.WithDemoOn(false),
		options.WithManagerOn(false),
		options.WithHTTPAddr(httpAddr),
		options.WithAddr("tcp://127.0.0.1:0"),
		options.WithWSAddr("ws://127.0.0.1:0"),
		options.WithClusterAddr(clusterAddr),
		options.WithClusterServerAddr(clusterAddr),
		options.WithClusterAPIURL("http://"+httpAddr),
		func(o *options.Options) { o.Plugin.SocketPath = filepath.Join(socketDir, "plugin.sock") },
	)
	require.NoError(t, s.Start())
	t.Cleanup(s.StopNoErr)
	s.MustWaitAllSlotsReady(10 * time.Second)
	return s, "http://" + httpAddr
}

func TestWebSocketBeforeAuthenticationUnregistersOnClose(t *testing.T) {
	s, _ := startConnectionFollowupTestServer(t)
	wsURL := "ws://" + s.engine.WSRealListenAddr().String()

	for _, tc := range []struct {
		name        string
		serverClose bool
	}{
		{name: "客户端断开"},
		{name: "服务端关闭", serverClose: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := make(map[int64]struct{})
			for _, conn := range service.ConnManager.GetAllConn() {
				old[conn.ID()] = struct{}{}
			}
			c, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Close() })
			conns := service.ConnManager.GetAllConn()
			require.Len(t, conns, len(old)+1)
			var id int64
			for _, conn := range conns {
				if _, exists := old[conn.ID()]; !exists {
					id = conn.ID()
					break
				}
			}
			require.NotZero(t, id)
			serverConn := service.ConnManager.GetConn(id)
			require.NotNil(t, serverConn)
			require.Nil(t, serverConn.Context(), "测试连接只完成 WS 握手，不发 IM 登录帧")

			if tc.serverClose {
				require.NoError(t, serverConn.Close())
			} else {
				require.NoError(t, c.Close())
			}
			require.Eventually(t, func() bool {
				return service.ConnManager.GetConn(id) == nil && service.ConnManager.ConnCount() == len(old)
			}, time.Second, 5*time.Millisecond, "已经关闭的连接不能留在实际连接登记表")
		})
	}
}

func TestLegacyCommandSyncKeepsUnreturnedChannelUnread(t *testing.T) {
	s, api := startConnectionFollowupTestServer(t)
	const uid = "legacy-sync-recipient"
	groupA := s.opts.OrginalConvertCmdChannel("legacy-a")
	groupB := s.opts.OrginalConvertCmdChannel("legacy-b")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	insert := func(channel string, startID int64, count int) {
		t.Helper()
		msgs := make([]wkdb.Message, 0, count)
		for i := 0; i < count; i++ {
			id := startID + int64(i)
			msgs = append(msgs, wkdb.Message{RecvPacket: wkproto.RecvPacket{
				Framer:      wkproto.Framer{SyncOnce: true},
				ChannelID:   channel,
				ChannelType: wkproto.ChannelTypeGroup,
				MessageID:   id,
				FromUID:     "legacy-sync-sender",
				ClientMsgNo: fmt.Sprintf("legacy-%d", id),
				Payload:     []byte(`{"type":1,"content":"local legacy sync fixture"}`),
			}})
		}
		_, err := service.Store.AppendMessages(ctx, channel, wkproto.ChannelTypeGroup, msgs)
		require.NoError(t, err)
	}
	insert(groupA, 1001, 3)
	insert(groupB, 1011, 1)
	require.NoError(t, service.Store.AddOrUpdateUserConversations(uid, []wkdb.Conversation{
		{Uid: uid, ChannelId: groupA, ChannelType: wkproto.ChannelTypeGroup, Type: wkdb.ConversationTypeCMD, ReadToMsgSeq: 1},
		{Uid: uid, ChannelId: groupB, ChannelType: wkproto.ChannelTypeGroup, Type: wkdb.ConversationTypeCMD},
	}))
	pull := func() []*types.MessageResp {
		t.Helper()
		status, raw := postJSONRaw(t, api+"/message/sync", map[string]interface{}{"uid": uid, "limit": 2})
		require.Equal(t, 200, status, "%s", raw)
		var messages []*types.MessageResp
		require.NoError(t, json.Unmarshal(raw, &messages))
		return messages
	}
	ack := func(seq uint64) {
		t.Helper()
		status, raw := postJSONRaw(t, api+"/message/syncack", map[string]interface{}{"uid": uid, "last_message_seq": seq})
		require.Equal(t, 200, status, "%s", raw)
	}
	cursor := func(channel string) uint64 {
		t.Helper()
		conversation, err := service.Store.GetConversation(uid, channel, wkproto.ChannelTypeGroup)
		require.NoError(t, err)
		return conversation.ReadToMsgSeq
	}

	// 第一页用完配额，确认它不能同时确认尚未返回的第二个群。
	first := pull()
	require.Len(t, first, 2)
	require.Equal(t, uint64(3), first[1].MessageSeq)
	ack(3)
	require.Equal(t, uint64(3), cursor(groupA))
	require.Zero(t, cursor(groupB))

	second := pull()
	require.Len(t, second, 1)
	require.Equal(t, uint64(1), second[0].MessageSeq)
	// 旧页的重复 ACK 不能消耗已经返回、但尚未确认的新页。
	ack(3)
	require.Zero(t, cursor(groupB))
	ack(1)
	require.Equal(t, uint64(1), cursor(groupB))
	require.Empty(t, pull())
}

func TestLegacyCommandSyncCanReturnNextChannelWithSameSequence(t *testing.T) {
	s, api := startConnectionFollowupTestServer(t)
	const uid = "legacy-same-sequence-recipient"
	first := s.opts.OrginalConvertCmdChannel("legacy-a")
	second := s.opts.OrginalConvertCmdChannel("legacy-b")
	seedFollowupCommands(t, uid, first, 2101, 1)
	seedFollowupCommands(t, uid, second, 2201, 1)

	page := pullFollowupCommands(t, api, uid, 1)
	require.Len(t, page, 1)
	require.Equal(t, int64(2101), page[0].MessageId)
	ackFollowupCommands(t, api, uid, 1)
	require.Equal(t, uint64(1), followupCommandCursor(t, uid, first))

	// 两群的第一条都是序号 1，确认前一群不应挡住后一群的消息。
	page = pullFollowupCommands(t, api, uid, 1)
	require.Len(t, page, 1)
	require.Equal(t, int64(2201), page[0].MessageId)
	ackFollowupCommands(t, api, uid, 1)
	require.Zero(t, followupCommandCursor(t, uid, second), "不能把相同序号的迟到回执误认成第二群的确认")
}

func TestLegacyCommandSyncRegistersOnlyVisiblePage(t *testing.T) {
	s, api := startConnectionFollowupTestServer(t)
	const uid = "legacy-visible-sequence-recipient"
	const firstGroup = "legacy-visible-a"
	first := s.opts.OrginalConvertCmdChannel(firstGroup)
	second := s.opts.OrginalConvertCmdChannel("legacy-visible-b")
	seedFollowupCommands(t, uid, first, 3101, 3)
	seedFollowupCommands(t, uid, second, 3201, 3)

	// 普通群聊的历史序号与 CMD 流不是同一套编号，不能过滤任何命令。
	page := pullFollowupVisibleCommands(t, api, uid, firstGroup, 2, 2)
	require.Len(t, page, 2)
	require.Equal(t, int64(3101), page[0].MessageId)
	require.Equal(t, int64(3102), page[1].MessageId)

	// 第一群前两条对客户端不可见，第二群的相同序号仍然能正常补拉。
	page = pullFollowupVisibleCommands(t, api, uid, first, 2, 2)
	require.Len(t, page, 2)
	require.Equal(t, int64(3103), page[0].MessageId)
	require.Equal(t, int64(3201), page[1].MessageId)
	ackFollowupCommands(t, api, uid, 1)
	require.Equal(t, uint64(3), followupCommandCursor(t, uid, first))
	require.Equal(t, uint64(1), followupCommandCursor(t, uid, second), "第二群尚未返回的游标不得推进")
	appendFollowupCommands(t, first, 3111, 1)

	// 最大历史偏移不能回绕，也不能屏蔽另一群仍待补拉的两条。
	page = pullFollowupVisibleCommands(t, api, uid, first, 10, ^uint64(0))
	require.Len(t, page, 2)
	require.Equal(t, int64(3202), page[0].MessageId)
	require.Equal(t, int64(3203), page[1].MessageId)
	ackFollowupCommands(t, api, uid, 3)
	require.Equal(t, uint64(3), followupCommandCursor(t, uid, first))
	require.Equal(t, uint64(3), followupCommandCursor(t, uid, second))
	page = pullFollowupCommands(t, api, uid, 10)
	require.Len(t, page, 1)
	require.Equal(t, int64(3111), page[0].MessageId)
}

func TestLegacyCommandSyncAfterOutOfOrderLiveAck(t *testing.T) {
	s, api := startConnectionFollowupTestServer(t)
	const uid = "legacy-ack-gap-recipient"
	channel := s.opts.OrginalConvertCmdChannel("legacy-ack-gap")
	seedFollowupCommands(t, uid, channel, 4101, 2)

	applyLiveAck := func(seq uint64) {
		t.Helper()
		require.NoError(t, service.Store.AdvanceCommandSyncCursorOnRecvack(uid, channel, wkproto.ChannelTypeGroup, seq))
		// 等待同一个用户槽应用后续日志，确保检测的是已落盘的游标。
		require.NoError(t, service.Store.AddOrUpdateUserConversations(uid, []wkdb.Conversation{
			{Uid: uid, ChannelId: "legacy-test-apply-barrier", ChannelType: wkproto.ChannelTypeGroup, Type: wkdb.ConversationTypeChat},
		}))
	}
	applyLiveAck(2)
	require.Zero(t, followupCommandCursor(t, uid, channel))
	page := pullFollowupCommands(t, api, uid, 1)
	require.Len(t, page, 1)
	require.Equal(t, int64(4101), page[0].MessageId)
	ackFollowupCommands(t, api, uid, 1)
	require.Equal(t, uint64(1), followupCommandCursor(t, uid, channel))
	applyLiveAck(2)
	require.Equal(t, uint64(2), followupCommandCursor(t, uid, channel))
	require.Empty(t, pullFollowupCommands(t, api, uid, 1))
}

func seedFollowupCommands(t *testing.T, uid, channel string, firstID int64, count int) {
	t.Helper()
	appendFollowupCommands(t, channel, firstID, count)
	require.NoError(t, service.Store.AddOrUpdateUserConversations(uid, []wkdb.Conversation{
		{Uid: uid, ChannelId: channel, ChannelType: wkproto.ChannelTypeGroup, Type: wkdb.ConversationTypeCMD},
	}))
}

func appendFollowupCommands(t *testing.T, channel string, firstID int64, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msgs := make([]wkdb.Message, 0, count)
	for i := 0; i < count; i++ {
		id := firstID + int64(i)
		msgs = append(msgs, wkdb.Message{RecvPacket: wkproto.RecvPacket{
			Framer:      wkproto.Framer{SyncOnce: true},
			ChannelID:   channel,
			ChannelType: wkproto.ChannelTypeGroup,
			MessageID:   id,
			FromUID:     "legacy-sync-sender",
			ClientMsgNo: fmt.Sprintf("legacy-%d", id),
			Payload:     []byte(`{"type":1,"content":"local legacy sync fixture"}`),
		}})
	}
	_, err := service.Store.AppendMessages(ctx, channel, wkproto.ChannelTypeGroup, msgs)
	require.NoError(t, err)
}

func pullFollowupCommands(t *testing.T, api, uid string, limit int) []*types.MessageResp {
	t.Helper()
	status, raw := postJSONRaw(t, api+"/message/sync", map[string]interface{}{"uid": uid, "limit": limit})
	require.Equal(t, 200, status, "%s", raw)
	var messages []*types.MessageResp
	require.NoError(t, json.Unmarshal(raw, &messages))
	return messages
}

func pullFollowupVisibleCommands(t *testing.T, api, uid, groupID string, limit int, afterSeq uint64) []*types.MessageResp {
	t.Helper()
	status, raw := postJSONRaw(t, api+"/message/sync", map[string]interface{}{
		"uid": uid, "limit": limit, "visibility": map[string]interface{}{
			"channel_id": groupID, "channel_type": wkproto.ChannelTypeGroup, "after_seq": afterSeq,
		},
	})
	require.Equal(t, 200, status, "%s", raw)
	var resp struct {
		Messages []*types.MessageResp `json:"messages"`
	}
	require.NoError(t, json.Unmarshal(raw, &resp))
	require.NotNil(t, resp.Messages)
	return resp.Messages
}

func ackFollowupCommands(t *testing.T, api, uid string, seq uint64) {
	t.Helper()
	status, raw := postJSONRaw(t, api+"/message/syncack", map[string]interface{}{"uid": uid, "last_message_seq": seq})
	require.Equal(t, 200, status, "%s", raw)
}

func followupCommandCursor(t *testing.T, uid, channel string) uint64 {
	t.Helper()
	conversation, err := service.Store.GetConversation(uid, channel, wkproto.ChannelTypeGroup)
	require.NoError(t, err)
	return conversation.ReadToMsgSeq
}
