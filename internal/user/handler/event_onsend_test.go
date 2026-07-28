package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestResolveSendChannelIDRejectsRaftKeyDelimiter(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		channelType uint8
		uid         string
	}{
		{name: "customer service raw", raw: "a&b", channelType: wkproto.ChannelTypeCustomerService, uid: "user"},
		{name: "person raw", raw: "peer&x", channelType: wkproto.ChannelTypePerson, uid: "user"},
		{name: "agent raw", raw: "agent&x", channelType: wkproto.ChannelTypeAgent, uid: "user"},
		{name: "person derived", raw: "peer", channelType: wkproto.ChannelTypePerson, uid: "user&x"},
		{name: "agent derived", raw: "agent", channelType: wkproto.ChannelTypeAgent, uid: "user&x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := resolveSendChannelID(tt.raw, tt.channelType, tt.uid); ok {
				t.Fatal("channel identity containing raft key delimiter must be rejected")
			}
		})
	}
}

func TestResolveSendChannelIDPreservesValidChannels(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		channelType uint8
		uid         string
		want        string
	}{
		{
			name:        "customer service",
			raw:         "support",
			channelType: wkproto.ChannelTypeCustomerService,
			uid:         "user",
			want:        "support",
		},
		{
			name:        "person",
			raw:         "peer",
			channelType: wkproto.ChannelTypePerson,
			uid:         "user",
			want:        options.GetFakeChannelIDWith("peer", "user"),
		},
		{
			name:        "agent",
			raw:         "agent",
			channelType: wkproto.ChannelTypeAgent,
			uid:         "user",
			want:        options.GetAgentChannelIDWith("user", "agent"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := resolveSendChannelID(tt.raw, tt.channelType, tt.uid)
			if !ok {
				t.Fatal("valid channel identity was rejected")
			}
			if got != tt.want {
				t.Fatalf("resolved channel ID = %q, want %q", got, tt.want)
			}
		})
	}
}
