package wkdb

import (
	"errors"
	"strings"
	"testing"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestHistoryFenceRaftReplayAndCorruptRecordFailClosed(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	messages := []Message{}
	for i := 1; i <= 2; i++ {
		messages = append(messages, Message{RecvPacket: wkproto.RecvPacket{Framer: wkproto.Framer{NoPersist: true, RedDot: true, SyncOnce: true}, MessageID: int64(i), MessageSeq: uint32(i), ChannelID: "a@c", ChannelType: 1, Payload: []byte("合成")}})
	}
	if err := db.AppendMessages("a@c", 1, messages); err != nil {
		t.Fatal(err)
	}
	fence, err := db.FreezeHistoryFence(HistoryFence{TaskID: strings.Repeat("a", 64), SourceUID: "a", PeerUID: "c", ChannelID: "a@c"})
	if err != nil {
		t.Fatal(err)
	}
	if fence.HighWatermark != 2 {
		t.Fatal(fence)
	}
	if err := db.AppendMessagesForRaft("a@c", 1, messages[:1]); err != nil {
		t.Fatal(err)
	}
	seq, _, err := db.GetChannelLastMessageSeq("a@c", 1)
	if err != nil || seq != 2 {
		t.Fatal(seq, err)
	}
	changed := messages[0]
	changed.Payload = []byte("不能覆盖")
	if err := db.AppendMessagesForRaft("a@c", 1, []Message{changed}); !errors.Is(err, ErrHistoryConflict) {
		t.Fatal(err)
	}
	changed.MessageSeq = 3
	if err := db.AppendMessagesForRaft("a@c", 1, []Message{changed}); !errors.Is(err, ErrHistoryFenced) {
		t.Fatal(err)
	}
	if err := db.TruncateLogTo("a@c", 1, 1); !errors.Is(err, ErrHistoryFenced) {
		t.Fatal("截断破坏固定水位", err)
	}
	if err := db.defaultShardDB().Set(historyFenceKey("a@c", 1), []byte("broken"), db.sync); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetHistoryFence("a@c", 1); err == nil {
		t.Fatal("损坏门禁未拒绝读取")
	}
	if err := db.AppendMessages("a@c", 1, messages); err == nil {
		t.Fatal("损坏门禁未拒写")
	}
}

func TestHistoryReleasedMetadataReplayCannotReviveOlderTasks(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var old []HistoryFence
	for _, letter := range []string{"a", "b", "c"} {
		f, err := db.FreezeHistoryFence(HistoryFence{TaskID: strings.Repeat(letter, 64), SourceUID: "a", PeerUID: "c", ChannelID: "a@c"})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.SetHistoryFence(f); err != nil {
			t.Fatal(err)
		}
		old = append(old, f)
		if letter != "c" {
			if err := db.ReleaseHistoryFence(f); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, f := range old[:2] {
		if err := db.SetHistoryFence(f); err != nil {
			t.Fatal(err)
		}
		if err := db.ReleaseHistoryFence(f); err != nil {
			t.Fatal(err)
		}
	}
	active, err := db.GetHistoryFence("a@c", 1)
	if err != nil || active == nil || active.TaskID != old[2].TaskID {
		t.Fatal(active, err)
	}
}

func TestHistoryEmptyFenceRejectsHiddenDataOrExistingConfig(t *testing.T) {
	for _, mode := range []string{"nonzero_watermark", "orphan_record", "existing_config", "empty"} {
		t.Run(mode, func(t *testing.T) {
			db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(1)))
			if err := db.Open(); err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			f := HistoryFence{TaskID: strings.Repeat("a", 64), SourceUID: "a", PeerUID: "c", ChannelID: "a@c"}
			if mode == "nonzero_watermark" || mode == "orphan_record" {
				msg := Message{RecvPacket: wkproto.RecvPacket{MessageID: 1, MessageSeq: 1, ChannelID: "a@c", ChannelType: 1, Payload: []byte("synthetic")}}
				if err := db.AppendMessages("a@c", 1, []Message{msg}); err != nil {
					t.Fatal(err)
				}
				if mode == "orphan_record" {
					if err := db.SetChannelLastMessageSeq("a@c", 1, 0); err != nil {
						t.Fatal(err)
					}
				}
			}
			if mode == "existing_config" {
				if err := db.SaveChannelClusterConfig(ChannelClusterConfig{ChannelId: "a@c", ChannelType: 1, LeaderId: 2}); err != nil {
					t.Fatal(err)
				}
			}
			got, err := db.(HistoryFenceStore).FreezeEmptyHistoryFence(f)
			if mode == "empty" {
				if err != nil || got.HighWatermark != 0 {
					t.Fatal(got, err)
				}
			} else if err == nil {
				t.Fatal("缺失配置分支误把非空/非本机来源当作空频道")
			}
		})
	}
}
