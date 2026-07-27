package api

import (
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestIndexFirstRecentMessagesKeepsFirstDuplicate(t *testing.T) {
	first := recentMessageForTest("group-1", wkproto.ChannelTypeGroup, 10)
	second := recentMessageForTest("group-1", wkproto.ChannelTypeGroup, 20)

	index := indexFirstRecentMessages([]*channelRecentMessage{first, second})
	got := index[conversationChannelKey{
		channelID:   "group-1",
		channelType: wkproto.ChannelTypeGroup,
	}]

	if got != first {
		t.Fatalf("duplicate channel selected %p, want first result %p", got, first)
	}
}

func TestIndexFirstRecentMessagesSeparatesChannelTypes(t *testing.T) {
	person := recentMessageForTest("same-id", wkproto.ChannelTypePerson, 10)
	group := recentMessageForTest("same-id", wkproto.ChannelTypeGroup, 20)

	index := indexFirstRecentMessages([]*channelRecentMessage{person, group})

	if got := index[conversationChannelKey{
		channelID:   "same-id",
		channelType: wkproto.ChannelTypePerson,
	}]; got != person {
		t.Fatalf("person lookup = %p, want %p", got, person)
	}
	if got := index[conversationChannelKey{
		channelID:   "same-id",
		channelType: wkproto.ChannelTypeGroup,
	}]; got != group {
		t.Fatalf("group lookup = %p, want %p", got, group)
	}
}

func TestIndexFirstRecentMessagesMatchesNestedLookup(t *testing.T) {
	userID := "user-a"
	peerID := "user-b"
	fakePersonChannelID := options.GetFakeChannelIDWith(userID, peerID)

	recentMessages := []*channelRecentMessage{
		recentMessageForTest("group-2", wkproto.ChannelTypeGroup, 20),
		recentMessageForTest(fakePersonChannelID, wkproto.ChannelTypePerson, 30),
		recentMessageForTest("group-1", wkproto.ChannelTypeGroup, 10),
		recentMessageForTest("group-1", wkproto.ChannelTypeGroup, 99),
	}
	keys := []conversationChannelKey{
		{channelID: "group-1", channelType: wkproto.ChannelTypeGroup},
		{channelID: fakePersonChannelID, channelType: wkproto.ChannelTypePerson},
		{channelID: "missing", channelType: wkproto.ChannelTypeGroup},
		{channelID: "group-2", channelType: wkproto.ChannelTypeGroup},
	}

	want := nestedRecentMessageLookup(recentMessages, keys)
	index := indexFirstRecentMessages(recentMessages)
	got := indexedRecentMessageLookup(index, keys)

	if len(got) != len(want) {
		t.Fatalf("lookup length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("lookup[%d] = %p, want %p", i, got[i], want[i])
		}
	}
}

func TestIndexFirstRecentMessagesMatchesNestedLookupAtScale(t *testing.T) {
	for _, size := range []int{0, 1, 100, 500, 1000} {
		t.Run(fmt.Sprintf("%d_channels", size), func(t *testing.T) {
			recentMessages := make([]*channelRecentMessage, 0, size)
			keys := make([]conversationChannelKey, 0, size)
			for i := 0; i < size; i++ {
				channelID := fmt.Sprintf("channel-%d", i)
				recentMessages = append(
					recentMessages,
					recentMessageForTest(
						channelID,
						wkproto.ChannelTypeGroup,
						uint64(i+1),
					),
				)
			}
			for i := size - 1; i >= 0; i-- {
				keys = append(keys, conversationChannelKey{
					channelID:   fmt.Sprintf("channel-%d", i),
					channelType: wkproto.ChannelTypeGroup,
				})
			}

			want := nestedRecentMessageLookup(recentMessages, keys)
			got := indexedRecentMessageLookup(
				indexFirstRecentMessages(recentMessages),
				keys,
			)
			for i := range want {
				if got[i] != want[i] {
					t.Fatalf(
						"lookup[%d] = %p, want %p",
						i,
						got[i],
						want[i],
					)
				}
			}
		})
	}
}

func TestIndexConversationChannelsSeparatesChannelTypes(t *testing.T) {
	conversations := []wkdb.Conversation{
		{ChannelId: "same-id", ChannelType: wkproto.ChannelTypePerson},
		{ChannelId: "same-id", ChannelType: wkproto.ChannelTypeGroup},
	}

	index := indexConversationChannels(conversations)
	for _, conversation := range conversations {
		key := conversationChannelKey{
			channelID:   conversation.ChannelId,
			channelType: conversation.ChannelType,
		}
		if _, exists := index[key]; !exists {
			t.Fatalf("conversation key %+v is missing", key)
		}
	}
}

func BenchmarkConversationRecentMessageLookup(b *testing.B) {
	for _, size := range []int{100, 500, 1000} {
		recentMessages := make([]*channelRecentMessage, 0, size)
		keys := make([]conversationChannelKey, 0, size)
		for i := 0; i < size; i++ {
			channelID := fmt.Sprintf("channel-%d", i)
			recentMessages = append(
				recentMessages,
				recentMessageForTest(channelID, wkproto.ChannelTypeGroup, uint64(i+1)),
			)
			keys = append(keys, conversationChannelKey{
				channelID:   channelID,
				channelType: wkproto.ChannelTypeGroup,
			})
		}

		b.Run(fmt.Sprintf("nested/%d", size), func(b *testing.B) {
			var result []*channelRecentMessage
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				result = nestedRecentMessageLookup(recentMessages, keys)
			}
			if len(result) != size {
				b.Fatalf("nested lookup returned %d results, want %d", len(result), size)
			}
		})

		b.Run(fmt.Sprintf("indexed/%d", size), func(b *testing.B) {
			var result []*channelRecentMessage
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				index := indexFirstRecentMessages(recentMessages)
				result = indexedRecentMessageLookup(index, keys)
			}
			if len(result) != size {
				b.Fatalf("indexed lookup returned %d results, want %d", len(result), size)
			}
		})
	}
}

func recentMessageForTest(
	channelID string,
	channelType uint8,
	messageSeq uint64,
) *channelRecentMessage {
	return &channelRecentMessage{
		ChannelId:   channelID,
		ChannelType: channelType,
		Messages: []*types.MessageResp{
			{MessageSeq: messageSeq},
		},
	}
}

func nestedRecentMessageLookup(
	recentMessages []*channelRecentMessage,
	keys []conversationChannelKey,
) []*channelRecentMessage {
	result := make([]*channelRecentMessage, 0, len(keys))
	for _, key := range keys {
		var matched *channelRecentMessage
		for _, recentMessage := range recentMessages {
			if recentMessage == nil {
				continue
			}
			if key.channelID == recentMessage.ChannelId &&
				key.channelType == recentMessage.ChannelType {
				matched = recentMessage
				break
			}
		}
		result = append(result, matched)
	}
	return result
}

func indexedRecentMessageLookup(
	index map[conversationChannelKey]*channelRecentMessage,
	keys []conversationChannelKey,
) []*channelRecentMessage {
	result := make([]*channelRecentMessage, 0, len(keys))
	for _, key := range keys {
		result = append(result, index[key])
	}
	return result
}
