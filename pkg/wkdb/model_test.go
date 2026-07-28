package wkdb

import (
	"bytes"
	"testing"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/assert"
)

func TestMessageUnmarshal(t *testing.T) {
	msg := &Message{
		RecvPacket: wkproto.RecvPacket{
			ChannelID:   "channel",
			ChannelType: 2,
			MessageSeq:  1,
			Payload:     []byte("hello"),
			MessageID:   1234,
		},
		Term: 100,
	}

	data, err := msg.Marshal()
	assert.NoError(t, err)

	newMsg := &Message{}
	err = newMsg.Unmarshal(data)
	assert.NoError(t, err)

	assert.Equal(t, msg.Payload, newMsg.Payload)
	assert.Equal(t, msg.ChannelID, newMsg.ChannelID)
	assert.Equal(t, msg.ChannelType, newMsg.ChannelType)
	assert.Equal(t, msg.MessageSeq, newMsg.MessageSeq)
	assert.Equal(t, msg.MessageID, newMsg.MessageID)
	assert.Equal(t, msg.Term, newMsg.Term)
}

func TestMessageLegacyEnvelopeDefaultsSearchOutboxFalse(t *testing.T) {
	legacy := Message{RecvPacket: wkproto.RecvPacket{
		MessageID: 7, ClientMsgNo: "legacy", ChannelID: "channel",
		ChannelType: 2, Payload: []byte("legacy"),
	}, Term: 3}
	data, err := legacy.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	var decoded Message
	if err := decoded.Unmarshal(data); err != nil {
		t.Fatal(err)
	}
	if decoded.SearchOutbox {
		t.Fatal("legacy envelope enabled SearchOutbox")
	}
}

func TestMessageSearchOutboxRoundTrip(t *testing.T) {
	want := Message{RecvPacket: wkproto.RecvPacket{
		MessageID: 8, ClientMsgNo: "new", ChannelID: "channel",
		ChannelType: 2, Payload: []byte("new"),
	}, Term: 4, SearchOutbox: true}
	data, err := want.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	var got Message
	if err := got.Unmarshal(data); err != nil {
		t.Fatal(err)
	}
	if !got.SearchOutbox || got.MessageID != want.MessageID ||
		!bytes.Equal(got.Payload, want.Payload) || got.Term != want.Term {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestMessageSearchOutboxEnvelopeIsAcceptedByLockedLegacyDecoder(t *testing.T) {
	current := Message{RecvPacket: wkproto.RecvPacket{
		MessageID: 9, ClientMsgNo: "cross-version", ChannelID: "channel",
		ChannelType: 2, Payload: []byte("body"),
	}, Term: 5, SearchOutbox: true}
	data, err := current.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := decodeLockedLegacyMessage(data)
	if err != nil {
		t.Fatal(err)
	}
	if legacy.MessageID != current.MessageID ||
		legacy.ClientMsgNo != current.ClientMsgNo ||
		!bytes.Equal(legacy.Payload, current.Payload) ||
		legacy.Term != current.Term {
		t.Fatalf("legacy decode = %+v, want message data preserved", legacy)
	}
}

func decodeLockedLegacyMessage(data []byte) (Message, error) {
	dec := wkproto.NewDecoder(data)
	version, err := dec.Uint8()
	if err != nil {
		return Message{}, err
	}
	const fixVersion = uint8(100)
	newEncode := version > fixVersion
	if newEncode {
		version -= fixVersion
	}
	var packetData []byte
	if newEncode {
		if _, err := dec.Uint8(); err != nil {
			return Message{}, err
		}
		length, err := dec.Uint32()
		if err != nil {
			return Message{}, err
		}
		packetData, err = dec.Bytes(int(length))
		if err != nil {
			return Message{}, err
		}
	} else {
		packetData, err = dec.Binary()
		if err != nil {
			return Message{}, err
		}
	}
	frame, _, err := proto.DecodeFrame(packetData, version)
	if err != nil {
		return Message{}, err
	}
	term, err := dec.Uint64()
	if err != nil {
		return Message{}, err
	}
	return Message{RecvPacket: *frame.(*wkproto.RecvPacket), Term: term}, nil
}
