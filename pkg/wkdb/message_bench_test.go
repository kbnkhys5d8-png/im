package wkdb_test

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func BenchmarkLoadPrevRangeMsgs100(b *testing.B) {
	d, channelID, channelType := newMessagePullBenchmarkDB(b)

	var (
		messages []wkdb.Message
		err      error
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messages, err = d.LoadPrevRangeMsgs(channelID, channelType, 1000, 0, 100)
	}
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	if len(messages) != 100 {
		b.Fatalf("LoadPrevRangeMsgs returned %d messages, want 100", len(messages))
	}
}

func BenchmarkLoadNextRangeMsgs100(b *testing.B) {
	d, channelID, channelType := newMessagePullBenchmarkDB(b)

	var (
		messages []wkdb.Message
		err      error
	)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		messages, err = d.LoadNextRangeMsgs(channelID, channelType, 901, 0, 100)
	}
	b.StopTimer()
	if err != nil {
		b.Fatal(err)
	}
	if len(messages) != 100 {
		b.Fatalf("LoadNextRangeMsgs returned %d messages, want 100", len(messages))
	}
}

func newMessagePullBenchmarkDB(b *testing.B) (wkdb.DB, string, uint8) {
	b.Helper()

	d := newTestDB(b)
	if err := d.Open(); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := d.Close(); err != nil {
			b.Error(err)
		}
	})

	const (
		channelID    = "message-pull-benchmark"
		channelType  = uint8(2)
		messageCount = 1000
	)
	messages := make([]wkdb.Message, 0, messageCount)
	for i := 1; i <= messageCount; i++ {
		messages = append(messages, wkdb.Message{
			RecvPacket: wkproto.RecvPacket{
				ChannelID:   channelID,
				ChannelType: channelType,
				MessageSeq:  uint32(i),
				Payload:     []byte("benchmark"),
			},
		})
	}
	if err := d.AppendMessages(channelID, channelType, messages); err != nil {
		b.Fatal(err)
	}
	return d, channelID, channelType
}
