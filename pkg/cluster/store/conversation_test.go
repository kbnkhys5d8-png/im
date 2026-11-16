package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestCommandSyncCursorUsesRecipientSlotAndPreservesOfflineGap(t *testing.T) {
	const uid, groupID, personID, chatID = "sync-recipient", "sync-group-cmd", "sync-person-cmd", "sync-chat"
	const channelType = wkproto.ChannelTypeGroup
	userStore, groupStore := newCommandCursorTestStore(t), newCommandCursorTestStore(t)
	// 将用户槽和群槽放在不共享数据的两个节点，避免单节点测试掩盖路由错误。
	slot := &commandCursorTestSlots{bySlot: map[uint32]*Store{1: userStore, 2: groupStore}, uid: uid}
	remoteStore := New(NewOptions(WithSlot(slot)))
	require.NoError(t, userStore.wdb.AddOrUpdateConversationsWithUser(uid, []wkdb.Conversation{
		{Id: 1, Uid: uid, ChannelId: groupID, ChannelType: channelType, Type: wkdb.ConversationTypeCMD, ReadToMsgSeq: 18},
		{Id: 2, Uid: uid, ChannelId: personID, ChannelType: wkproto.ChannelTypePerson, Type: wkdb.ConversationTypeCMD, ReadToMsgSeq: 7},
		{Id: 3, Uid: uid, ChannelId: chatID, ChannelType: channelType, Type: wkdb.ConversationTypeChat, ReadToMsgSeq: 4},
	}))
	readCursor := func(channelID string, typ uint8) uint64 {
		t.Helper()
		conversation, err := userStore.GetConversation(uid, channelID, typ)
		require.NoError(t, err)
		return conversation.ReadToMsgSeq
	}
	// 19 在离线期间未收到；重连后对 20 的实时回执不能跳过 19。
	require.NoError(t, remoteStore.AdvanceCommandSyncCursorOnRecvack(uid, groupID, channelType, 20))
	require.EqualValues(t, 18, readCursor(groupID, channelType))
	require.NoError(t, remoteStore.AdvanceCommandSyncCursors(uid, []wkdb.CommandSyncCursor{
		{ChannelID: groupID, ChannelType: channelType, Seq: 19},
		{ChannelID: personID, ChannelType: wkproto.ChannelTypePerson, Seq: 13},
		{ChannelID: personID, ChannelType: wkproto.ChannelTypePerson, Seq: 11},
		{ChannelID: chatID, ChannelType: channelType, Seq: 88},
	}))
	require.Equal(t, []uint32{1}, slot.synchronousDestinations)
	require.EqualValues(t, 19, readCursor(groupID, channelType))
	require.EqualValues(t, 13, readCursor(personID, wkproto.ChannelTypePerson))
	require.EqualValues(t, 4, readCursor(chatID, channelType))
	require.NoError(t, remoteStore.AdvanceCommandSyncCursorOnRecvack(uid, groupID, channelType, 20))
	require.EqualValues(t, 20, readCursor(groupID, channelType))
	require.NoError(t, remoteStore.AdvanceCommandSyncCursors(uid, []wkdb.CommandSyncCursor{{ChannelID: groupID, ChannelType: channelType, Seq: 19}}))
	require.EqualValues(t, 20, readCursor(groupID, channelType))
	require.Equal(t, []uint32{1, 1, 1, 1}, slot.destinations)
	require.Equal(t, []uint32{1, 1}, slot.synchronousDestinations)
	_, err := groupStore.GetConversation(uid, groupID, channelType)
	require.ErrorIs(t, err, wkdb.ErrNotFound)
}

func TestCommandSyncCursorIsAppliedAfterEarlierConversationCreation(t *testing.T) {
	s := newCommandCursorTestStore(t)
	const uid, groupID = "batched-recipient", "batched-group-cmd"
	const channelType = wkproto.ChannelTypeGroup
	create, err := EncodeCMDAddOrUpdateConversations([]wkdb.Conversation{{
		Id: 1, Uid: uid, ChannelId: groupID, ChannelType: channelType, Type: wkdb.ConversationTypeCMD,
	}})
	require.NoError(t, err)
	cmds := []*CMD{
		NewCMD(CMDAddOrUpdateConversationsBatchIfNotExist, create),
		NewCMD(CMDAdvanceCommandSyncCursor, encodeCMDAdvanceCommandSyncCursors(uid, []wkdb.CommandSyncCursor{{ChannelID: groupID, ChannelType: channelType, Seq: 3}})),
		NewCMD(CMDAdvanceCommandSyncCursorOnRecvack, EncodeCMDUpdateConversationIfSeqGreater(uid, groupID, channelType, 4)),
	}
	for len(cmds) < 10 {
		cmds = append(cmds, NewCMD(CMDAdvanceCommandSyncCursorOnRecvack, EncodeCMDUpdateConversationIfSeqGreater(uid, groupID, channelType, 1)))
	}
	logs := make([]types.Log, 0, len(cmds))
	for i, cmd := range cmds {
		data, err := cmd.Marshal()
		require.NoError(t, err)
		logs = append(logs, types.Log{Index: uint64(i + 1), Data: data})
	}
	require.NoError(t, s.ApplySlotLogs(1, logs))
	conversation, err := s.GetConversation(uid, groupID, channelType)
	require.NoError(t, err)
	require.EqualValues(t, 4, conversation.ReadToMsgSeq)
}

func newCommandCursorTestStore(t *testing.T) *Store {
	t.Helper()
	previousTrace := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previousTrace) })
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	require.NoError(t, db.Open())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return New(NewOptions(WithDB(db)))
}

type commandCursorTestSlots struct {
	icluster.Slot
	bySlot                  map[uint32]*Store
	uid                     string
	destinations            []uint32
	synchronousDestinations []uint32
}

func (s *commandCursorTestSlots) GetSlotId(key string) uint32 {
	if key == s.uid {
		return 1
	}
	return 2
}

func (s *commandCursorTestSlots) Propose(slotID uint32, data []byte) (*types.ProposeResp, error) {
	s.destinations = append(s.destinations, slotID)
	node, ok := s.bySlot[slotID]
	if !ok {
		return nil, fmt.Errorf("未知测试槽：%d", slotID)
	}
	index := uint64(len(s.destinations))
	return &types.ProposeResp{Index: index}, node.ApplySlotLogs(slotID, []types.Log{{Index: index, Data: data}})
}

func (s *commandCursorTestSlots) ProposeUntilApplied(slotID uint32, data []byte) (*types.ProposeResp, error) {
	s.synchronousDestinations = append(s.synchronousDestinations, slotID)
	return s.Propose(slotID, data)
}
