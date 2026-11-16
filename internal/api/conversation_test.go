package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestConversationUnreadRoutesOnlyUpdateChat(t *testing.T) {
	oldOptions, oldStore, oldCluster := options.G, service.Store, service.Cluster
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	options.G.Channel.CmdSuffix = "_cmd_test"
	t.Cleanup(func() { options.G, service.Store, service.Cluster = oldOptions, oldStore, oldCluster })

	const uid = "unread-user"
	const peer = "unread-peer"
	const mappedCmdPeer = "unread-peer-2"
	privateID := options.GetFakeChannelIDWith(uid, peer)
	groupCmd := options.G.OrginalConvertCmdChannel("unread-group")
	privateCmd := options.G.OrginalConvertCmdChannel(privateID)
	peerWithSuffix := options.G.OrginalConvertCmdChannel(peer)
	privateIDWithSuffixPeer := options.GetFakeChannelIDWith(uid, peerWithSuffix)
	cmdUID := options.G.OrginalConvertCmdChannel(uid)
	if !options.G.IsCmdChannel(options.GetFakeChannelIDWith(cmdUID, mappedCmdPeer)) {
		t.Fatal("测试私聊必须仅在映射后得到带命令后缀的存储键")
	}
	if options.G.IsCmdChannel(privateIDWithSuffixPeer) {
		t.Fatal("普通对端 UID 可以带后缀，但该用例映射后的存储键必须不是 CMD")
	}
	for _, route := range []struct {
		name, path string
		wantSeq    uint64
	}{
		{"清空未读", "/conversations/clearUnread", 10},
		{"设置未读", "/conversations/setUnread", 7},
	} {
		t.Run(route.name, func(t *testing.T) {
			for _, tc := range []struct {
				name, inputID, storageID, uid string
				channelType                   uint8
				conversationType              wkdb.ConversationType
				missing, blocked              bool
			}{
				{name: "普通群聊", inputID: "unread-group", storageID: "unread-group", channelType: wkproto.ChannelTypeGroup},
				{name: "普通私聊", inputID: peer, storageID: privateID, channelType: wkproto.ChannelTypePerson},
				{name: "普通私聊对端 UID 带同名后缀", inputID: peerWithSuffix, storageID: privateIDWithSuffixPeer, channelType: wkproto.ChannelTypePerson},
				{name: "普通会话不存在时仍可创建", inputID: peer, storageID: privateID, channelType: wkproto.ChannelTypePerson, missing: true},
				{name: "CMD 群标识不读取也不写入", inputID: groupCmd, channelType: wkproto.ChannelTypeGroup, blocked: true},
				{name: "CMD 私聊物理标识不接受二次映射", inputID: privateCmd, channelType: wkproto.ChannelTypePerson, blocked: true},
				{name: "私聊映射后的 CMD 存储键不读写", uid: cmdUID, inputID: mappedCmdPeer, channelType: wkproto.ChannelTypePerson, blocked: true},
				{name: "无后缀的历史 CMD 记录也不写入", inputID: peer, storageID: privateID, channelType: wkproto.ChannelTypePerson, conversationType: wkdb.ConversationTypeCMD, blocked: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					requestUID := tc.uid
					if requestUID == "" {
						requestUID = uid
					}
					cluster := &conversationUnreadCluster{}
					service.Cluster = cluster
					db := &conversationUnreadDB{t: t, uid: requestUID, channelID: tc.storageID, channelType: tc.channelType, missing: tc.missing, conversationType: tc.conversationType, lastMsgSeq: 10}
					slot := &conversationUnreadSlot{}
					service.Store = store.New(store.NewOptions(store.WithDB(db), store.WithSlot(slot)))
					router := wkhttp.New()
					newConversation(nil).route(router)
					body, err := json.Marshal(map[string]any{"uid": requestUID, "channel_id": tc.inputID, "channel_type": tc.channelType, "unread": 3})
					if err != nil {
						t.Fatal(err)
					}
					req := httptest.NewRequest(http.MethodPost, route.path, bytes.NewReader(body))
					response := httptest.NewRecorder()
					router.GetGinRoute().ServeHTTP(response, req)

					if tc.blocked {
						if response.Code != http.StatusBadRequest || !bytes.Contains(response.Body.Bytes(), []byte(errUnreadCommandConversation.Error())) {
							t.Fatalf("期望拒绝命令会话，实际状态=%d，响应=%s", response.Code, response.Body)
						}
						if tc.storageID == "" && (cluster.lookups != 0 || db.conversationReads != 0 || db.sequenceReads != 0) {
							t.Fatal("识别为命令频道后不应查集群路由、会话或尾序号")
						}
						if len(slot.writes) != 0 {
							t.Fatalf("拒绝后不应写会话：写入=%v", slot.writes)
						}
						return
					}
					if response.Code != http.StatusOK || len(slot.writes) != 1 {
						t.Fatalf("普通未读操作未成功：状态=%d，响应=%s，写入=%v", response.Code, response.Body, slot.writes)
					}
					got := slot.writes[0]
					if slot.uid != requestUID || got.ChannelId != tc.storageID || got.ChannelType != tc.channelType || got.Type != wkdb.ConversationTypeChat || got.ReadToMsgSeq != route.wantSeq {
						t.Fatalf("普通会话写入不正确：%+v，期望序号=%d", got, route.wantSeq)
					}
				})
			}
		})
	}
}

func TestSetConversationUnreadSequenceBoundaries(t *testing.T) {
	oldOptions, oldStore, oldCluster := options.G, service.Store, service.Cluster
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Cluster = &conversationUnreadCluster{}
	t.Cleanup(func() { options.G, service.Store, service.Cluster = oldOptions, oldStore, oldCluster })

	const uid, peer = "unread-boundary-user", "unread-boundary-peer"
	for _, tc := range []struct {
		name             string
		channelType      uint8
		lastMsgSeq       uint64
		unread           int
		missing          bool
		conversationType wkdb.ConversationType
		wantStatus       int
		wantWrite        uint64
	}{
		{name: "空群聊保留已存在会话", channelType: wkproto.ChannelTypeGroup, unread: 3, wantStatus: http.StatusOK},
		{name: "空私聊不创建会话", channelType: wkproto.ChannelTypePerson, unread: 3, missing: true, wantStatus: http.StatusOK},
		{name: "空频道设置零未读也不写入", channelType: wkproto.ChannelTypeGroup, wantStatus: http.StatusOK},
		{name: "空频道仍拒绝历史 CMD 会话", channelType: wkproto.ChannelTypeGroup, unread: 3, conversationType: wkdb.ConversationTypeCMD, wantStatus: http.StatusBadRequest},
		{name: "首条消息等于未读数时不推进", channelType: wkproto.ChannelTypePerson, lastMsgSeq: 1, unread: 1, missing: true, wantStatus: http.StatusOK},
		{name: "非空频道超量未读保留旧算法", channelType: wkproto.ChannelTypeGroup, lastMsgSeq: 10, unread: 11, wantStatus: http.StatusOK, wantWrite: 9},
		{name: "非空频道设置零未读推进到末条", channelType: wkproto.ChannelTypeGroup, lastMsgSeq: 10, wantStatus: http.StatusOK, wantWrite: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			channelID := peer
			if tc.channelType == wkproto.ChannelTypePerson {
				channelID = options.GetFakeChannelIDWith(uid, peer)
			}
			db := &conversationUnreadDB{
				t: t, uid: uid, channelID: channelID, channelType: tc.channelType,
				missing: tc.missing, conversationType: tc.conversationType, lastMsgSeq: tc.lastMsgSeq,
			}
			slot := &conversationUnreadSlot{}
			service.Store = store.New(store.NewOptions(store.WithDB(db), store.WithSlot(slot)))
			router := wkhttp.New()
			newConversation(nil).route(router)
			body, err := json.Marshal(map[string]any{"uid": uid, "channel_id": peer, "channel_type": tc.channelType, "unread": tc.unread})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/conversations/setUnread", bytes.NewReader(body))
			response := httptest.NewRecorder()
			router.GetGinRoute().ServeHTTP(response, req)

			if response.Code != tc.wantStatus {
				t.Fatalf("状态码=%d，期望=%d，响应=%s", response.Code, tc.wantStatus, response.Body)
			}
			if tc.wantWrite == 0 {
				if len(slot.writes) != 0 {
					t.Fatalf("不应写入未读游标，实际写入=%v", slot.writes)
				}
			} else if len(slot.writes) != 1 || slot.writes[0].ReadToMsgSeq != tc.wantWrite {
				t.Fatalf("实际写入=%v，期望游标=%d", slot.writes, tc.wantWrite)
			}
		})
	}
}

type conversationUnreadCluster struct {
	icluster.ICluster
	lookups int
}

func (c *conversationUnreadCluster) SlotLeaderOfChannel(string, uint8) (*nodetypes.Node, error) {
	c.lookups++
	return &nodetypes.Node{Id: 1}, nil
}

type conversationUnreadDB struct {
	wkdb.DB
	t                                *testing.T
	uid, channelID                   string
	channelType                      uint8
	conversationType                 wkdb.ConversationType
	missing                          bool
	lastMsgSeq                       uint64
	conversationReads, sequenceReads int
}

func (db *conversationUnreadDB) GetConversation(uid, channelID string, channelType uint8) (wkdb.Conversation, error) {
	db.conversationReads++
	if uid != db.uid || channelID != db.channelID || channelType != db.channelType {
		db.t.Errorf("查错会话：%s %s %d", uid, channelID, channelType)
	}
	if db.missing {
		return wkdb.EmptyConversation, wkdb.ErrNotFound
	}
	return wkdb.Conversation{Id: 1, Uid: uid, ChannelId: channelID, ChannelType: channelType, Type: db.conversationType, ReadToMsgSeq: 3}, nil
}

func (db *conversationUnreadDB) GetChannelLastMessageSeq(channelID string, channelType uint8) (uint64, uint64, error) {
	db.sequenceReads++
	if channelID != db.channelID || channelType != db.channelType {
		db.t.Errorf("查错频道尾序号：%s %d", channelID, channelType)
	}
	return db.lastMsgSeq, 0, nil
}

func (*conversationUnreadDB) NextPrimaryKey() uint64 { return 2 }

type conversationUnreadSlot struct {
	icluster.Slot
	uid    string
	writes []wkdb.Conversation
}

func (*conversationUnreadSlot) GetSlotId(string) uint32 { return 1 }

func (s *conversationUnreadSlot) ProposeUntilApplied(_ uint32, data []byte) (*rafttypes.ProposeResp, error) {
	cmd := &store.CMD{}
	if err := cmd.Unmarshal(data); err != nil {
		return nil, err
	}
	var err error
	s.uid, s.writes, err = cmd.DecodeCMDAddOrUpdateUserConversations()
	return nil, err
}
