package wkdb

import (
	"errors"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
)

func TestSearchMessagesChannelClientMsgNoUsesSecondaryIndex(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	const (
		channelID   = "indexed-channel"
		channelType = uint8(2)
		clientMsgNo = "duplicate-client-msg-no"
	)

	messages := make([]Message, 0, 582)
	for seq := uint32(1); seq <= 70; seq++ {
		messages = append(messages, searchIndexTestMessage(channelID, channelType, seq, clientMsgNo))
	}
	for seq := uint32(71); seq <= 582; seq++ {
		messages = append(messages, searchIndexTestMessage(channelID, channelType, seq, fmt.Sprintf("noise-%d", seq)))
	}
	if err := db.AppendMessages(channelID, channelType, messages); err != nil {
		t.Fatal(err)
	}

	if err := db.AppendMessages("other-channel", channelType, []Message{
		searchIndexTestMessage("other-channel", channelType, 1, clientMsgNo),
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.AppendMessages(channelID, channelType+1, []Message{
		searchIndexTestMessage(channelID, channelType+1, 1, clientMsgNo),
	}); err != nil {
		t.Fatal(err)
	}

	shard := db.channelDb(channelID, channelType)
	var collisionPrimary [16]byte
	db.endian.PutUint64(collisionPrimary[:8], key.ChannelToNum(channelID, channelType))
	db.endian.PutUint64(collisionPrimary[8:], 582)
	if err := shard.Set(key.NewMessageSecondIndexClientMsgNoKey(clientMsgNo, collisionPrimary), nil, pebble.Sync); err != nil {
		t.Fatal(err)
	}

	// A channel-table scan would encounter this malformed unrelated key and fail.
	// The client_msg_no index path must only decode indexed candidates.
	malformedPrimaryKey := append(key.NewMessagePrimaryKey(channelID, channelType, 500), 0x01)
	if err := shard.Set(malformedPrimaryKey, nil, pebble.Sync); err != nil {
		t.Fatal(err)
	}

	got, err := db.SearchMessages(MessageSearchReq{
		ChannelId:   channelID,
		ChannelType: channelType,
		ClientMsgNo: clientMsgNo,
		Limit:       65,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 65 {
		t.Fatalf("len(messages) = %d, want 65", len(got))
	}
	for _, msg := range got {
		if msg.ChannelID != channelID || msg.ChannelType != channelType || msg.ClientMsgNo != clientMsgNo {
			t.Fatalf("unexpected indexed result: channel=%q type=%d client_msg_no=%q", msg.ChannelID, msg.ChannelType, msg.ClientMsgNo)
		}
	}

	got, err = db.SearchMessages(MessageSearchReq{
		ChannelId:        channelID,
		ChannelType:      channelType,
		ClientMsgNo:      clientMsgNo,
		Limit:            3,
		OffsetMessageSeq: 5,
		Pre:              true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].MessageSeq != 6 || got[1].MessageSeq != 7 || got[2].MessageSeq != 8 {
		t.Fatalf("forward page sequences = %v, want [6 7 8]", messageSeqs(got))
	}

	got, err = db.SearchMessages(MessageSearchReq{
		ChannelId:        channelID,
		ChannelType:      channelType,
		ClientMsgNo:      clientMsgNo,
		Limit:            3,
		OffsetMessageSeq: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].MessageSeq != 9 || got[1].MessageSeq != 8 || got[2].MessageSeq != 7 {
		t.Fatalf("backward page sequences = %v, want [9 8 7]", messageSeqs(got))
	}
}

func TestSearchMessagesChannelClientMsgNoReturnsOnlySelectedSenderRetries(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	const (
		channelID   = "sender-collision-channel"
		channelType = uint8(2)
		clientMsgNo = "shared-client-msg-no"
	)
	messages := []Message{
		searchIndexTestMessageFrom(channelID, channelType, 1, clientMsgNo, "target-sender"),
		searchIndexTestMessageFrom(channelID, channelType, 2, clientMsgNo, "other-sender"),
		searchIndexTestMessageFrom(channelID, channelType, 3, clientMsgNo, "target-sender"),
		searchIndexTestMessageFrom(channelID, channelType, 4, clientMsgNo, "other-sender"),
	}
	if err := db.AppendMessages(channelID, channelType, messages); err != nil {
		t.Fatal(err)
	}

	got, err := db.SearchMessages(MessageSearchReq{
		ChannelId: channelID, ChannelType: channelType, ClientMsgNo: clientMsgNo,
		FromUid: "target-sender", Limit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].MessageSeq != 3 || got[1].MessageSeq != 1 {
		t.Fatalf("selected sender sequences = %v, want [3 1]", messageSeqs(got))
	}
	for _, msg := range got {
		if msg.FromUID != "target-sender" {
			t.Fatalf("unexpected sender %q in selected sender retries", msg.FromUID)
		}
	}
}

func TestSearchMessagesChannelClientMsgNoFailsClosedOnTooManyForeignSenderCandidates(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	const (
		channelID             = "foreign-candidate-overflow"
		channelType           = uint8(2)
		clientMsgNo           = "colliding-client-msg-no"
		foreignCandidateCount = 4097
	)
	messages := make([]Message, 0, foreignCandidateCount+1)
	messages = append(messages, searchIndexTestMessageFrom(channelID, channelType, 1, clientMsgNo, "target-sender"))
	for index := 0; index < foreignCandidateCount; index++ {
		messages = append(messages, searchIndexTestMessageFrom(channelID, channelType, uint32(index+2), clientMsgNo, "foreign-sender"))
	}
	if err := db.AppendMessages(channelID, channelType, messages); err != nil {
		t.Fatal(err)
	}

	got, err := db.SearchMessages(MessageSearchReq{
		ChannelId: channelID, ChannelType: channelType, ClientMsgNo: clientMsgNo,
		FromUid: "target-sender", Limit: 2,
	})
	if err == nil {
		t.Fatalf("candidate overflow returned %d partial message(s), want a fail-closed error", len(got))
	}
	if !errors.Is(err, ErrMessageSearchCandidateLimit) {
		t.Fatalf("candidate overflow error = %v, want ErrMessageSearchCandidateLimit", err)
	}
	if len(got) != 0 {
		t.Fatalf("candidate overflow returned %d partial message(s), want none", len(got))
	}
}

func searchIndexTestMessage(channelID string, channelType uint8, seq uint32, clientMsgNo string) Message {
	return searchIndexTestMessageFrom(channelID, channelType, seq, clientMsgNo, "")
}

func searchIndexTestMessageFrom(channelID string, channelType uint8, seq uint32, clientMsgNo, fromUID string) Message {
	return Message{RecvPacket: wkproto.RecvPacket{
		ChannelID:   channelID,
		ChannelType: channelType,
		MessageID:   int64(seq),
		MessageSeq:  seq,
		ClientMsgNo: clientMsgNo,
		FromUID:     fromUID,
		Payload:     []byte("payload"),
	}}
}

func messageSeqs(messages []Message) []uint32 {
	seqs := make([]uint32, 0, len(messages))
	for _, msg := range messages {
		seqs = append(seqs, msg.MessageSeq)
	}
	return seqs
}
