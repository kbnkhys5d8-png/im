package wkdb

import (
	"bytes"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
)

func TestLoadNextRangeSearchSourceMessagesPreservesSearchFields(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	message := Message{
		RecvPacket: wkproto.RecvPacket{
			Framer:      wkproto.Framer{NoPersist: true},
			Setting:     wkproto.SettingSignal,
			MessageID:   99,
			MessageSeq:  1,
			ClientMsgNo: "client-1",
			StreamNo:    "stream-1",
			Timestamp:   123,
			Expire:      456,
			FromUID:     "sender",
			ChannelID:   "channel",
			ChannelType: 2,
			Topic:       "topic",
			Payload:     []byte("payload"),
		},
		Term: 7,
	}
	if err := db.AppendMessages("channel", 2, []Message{message}); err != nil {
		t.Fatal(err)
	}

	got, err := db.LoadNextRangeSearchSourceMessages("channel", 2, 1, 2, 10, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len(messages) = %d, want 1", len(got))
	}
	if got[0].PayloadOmitted {
		t.Fatal("payload unexpectedly omitted")
	}
	if got[0].Message.Expire != 456 || got[0].Message.Setting != wkproto.SettingSignal || !got[0].Message.Framer.NoPersist {
		t.Fatalf("search fields were not preserved: %+v", got[0].Message.RecvPacket)
	}
	if got[0].Message.Term != 7 || !bytes.Equal(got[0].Message.Payload, []byte("payload")) {
		t.Fatalf("term/payload were not preserved: %+v", got[0])
	}
}

func TestLoadNextRangeSearchSourceMessagesOmitsOnlyOversizedPayload(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})

	message := Message{RecvPacket: wkproto.RecvPacket{
		MessageID: 1, MessageSeq: 1, ClientMsgNo: "client", FromUID: "sender",
		ChannelID: "channel", ChannelType: 2, Payload: bytes.Repeat([]byte("x"), 1024),
	}}
	if err := db.AppendMessages("channel", 2, []Message{message}); err != nil {
		t.Fatal(err)
	}

	got, err := db.LoadNextRangeSearchSourceMessages("channel", 2, 1, 2, 1, 128)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got[0].PayloadOmitted || got[0].Message.Payload != nil {
		t.Fatalf("oversized payload result = %+v", got)
	}
	if _, err := db.LoadNextRangeSearchSourceMessages("channel", 2, 1, 2, 1, 1); !errors.Is(err, ErrSearchSourceMessageResponseBudget) {
		t.Fatalf("metadata budget error = %v", err)
	}
}

func TestLoadNextRangeSearchSourceMessagesRejectsUnboundedInputs(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if _, err := db.LoadNextRangeSearchSourceMessages("channel", 2, 0, 2, 1, 1); err == nil {
		t.Fatal("zero start sequence was accepted")
	}
	if _, err := db.LoadNextRangeSearchSourceMessages("channel", 2, 1, 2, 501, 1); err == nil {
		t.Fatal("oversized page was accepted")
	}
	if _, err := db.LoadNextRangeSearchSourceMessages("channel", 2, 1, 2, 1, 0); err == nil {
		t.Fatal("zero byte budget was accepted")
	}
}

func TestLoadNextRangeSearchSourceMessagesRejectsMissingRequiredColumn(t *testing.T) {
	db := newSearchSourceMessageTestDB(t)
	appendSearchSourceMessageTestRow(t, db)
	if err := db.channelDb("channel", 2).Delete(
		key.NewMessageColumnKey("channel", 2, 1, key.TableMessage.Column.Expire),
		pebble.Sync,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.LoadNextRangeSearchSourceMessages("channel", 2, 1, 2, 1, 1024); err == nil {
		t.Fatal("message missing required expire column was accepted")
	}
}

func TestLoadNextRangeSearchSourceMessagesRejectsUnknownColumn(t *testing.T) {
	db := newSearchSourceMessageTestDB(t)
	appendSearchSourceMessageTestRow(t, db)
	if err := db.channelDb("channel", 2).Set(
		key.NewMessageColumnKey("channel", 2, 1, [2]byte{0x7f, 0x7f}),
		[]byte("future-sensitive-value"),
		pebble.Sync,
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.LoadNextRangeSearchSourceMessages("channel", 2, 1, 2, 1, 1024); err == nil {
		t.Fatal("unknown message column was accepted")
	}
}

func newSearchSourceMessageTestDB(t *testing.T) *wukongDB {
	t.Helper()
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	})
	return db
}

func appendSearchSourceMessageTestRow(t *testing.T, db *wukongDB) {
	t.Helper()
	if err := db.AppendMessages("channel", 2, []Message{{RecvPacket: wkproto.RecvPacket{
		MessageID: 1, MessageSeq: 1, ClientMsgNo: "client", ChannelID: "channel",
		ChannelType: 2, FromUID: "sender", Payload: []byte("payload"),
	}}}); err != nil {
		t.Fatal(err)
	}
}
