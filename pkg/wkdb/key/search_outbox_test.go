package key

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"
)

func TestSearchOutboxKeyRoundTripUsesExactIdentity(t *testing.T) {
	keyBytes, err := NewSearchOutboxKey("a/b:用户", 2, 17, 99)
	if err != nil {
		t.Fatal(err)
	}
	channelID, channelType, messageSeq, messageID, err := ParseSearchOutboxKey(keyBytes)
	if err != nil {
		t.Fatal(err)
	}
	if channelID != "a/b:用户" || channelType != 2 || messageSeq != 17 || messageID != 99 {
		t.Fatalf("decoded = %q/%d/%d/%d", channelID, channelType, messageSeq, messageID)
	}
}

func TestSearchOutboxChannelRangeExcludesNeighboringChannel(t *testing.T) {
	lower, upper, err := NewSearchOutboxChannelRange("channel-a", 2, 11)
	if err != nil {
		t.Fatal(err)
	}
	inRange, err := NewSearchOutboxKey("channel-a", 2, 12, 1)
	if err != nil {
		t.Fatal(err)
	}
	neighbor, err := NewSearchOutboxKey("channel-b", 2, 12, 1)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(inRange, lower) < 0 || bytes.Compare(inRange, upper) >= 0 {
		t.Fatal("same-channel key is outside range")
	}
	if bytes.Compare(neighbor, lower) >= 0 && bytes.Compare(neighbor, upper) < 0 {
		t.Fatal("neighboring channel key entered exact range")
	}
}

func TestSearchOutboxKeyRejectsInvalidIdentity(t *testing.T) {
	tests := []struct {
		name        string
		channelID   string
		channelType uint8
		messageSeq  uint64
		messageID   int64
	}{
		{name: "empty channel", channelType: 2, messageSeq: 1, messageID: 1},
		{name: "zero type", channelID: "channel", messageSeq: 1, messageID: 1},
		{name: "zero sequence", channelID: "channel", channelType: 2, messageID: 1},
		{name: "zero message id", channelID: "channel", channelType: 2, messageSeq: 1},
		{name: "negative message id", channelID: "channel", channelType: 2, messageSeq: 1, messageID: -1},
		{
			name: "oversized channel", channelID: strings.Repeat("x", MaxSearchOutboxChannelIDBytes+1),
			channelType: 2, messageSeq: 1, messageID: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewSearchOutboxKey(test.channelID, test.channelType, test.messageSeq, test.messageID); err == nil {
				t.Fatal("NewSearchOutboxKey error = nil")
			}
		})
	}
}

func TestSearchOutboxKeyRejectsMalformedEncoding(t *testing.T) {
	valid, err := NewSearchOutboxKey("channel", 2, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	badLength := append([]byte(nil), valid...)
	binary.BigEndian.PutUint16(badLength[5:7], uint16(len("channel")+1))

	for name, raw := range map[string][]byte{
		"short":           valid[:len(valid)-1],
		"bad channel len": badLength,
		"trailing bytes":  append(append([]byte(nil), valid...), 0),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, _, err := ParseSearchOutboxKey(raw); err == nil {
				t.Fatal("ParseSearchOutboxKey error = nil")
			}
		})
	}
}
