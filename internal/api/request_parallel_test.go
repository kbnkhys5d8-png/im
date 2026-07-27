package api

import (
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
)

func TestRequestPeerRecentMessagesSkipsLocalAndCollectsRemote(t *testing.T) {
	const localNodeID uint64 = 1
	groups := map[uint64][]*channelRecentMessageReq{
		localNodeID: {{ChannelId: "local"}},
		2:           {{ChannelId: "remote-2"}},
		3:           {{ChannelId: "remote-3"}},
	}

	var calls atomic.Int32
	results, err := requestPeerRecentMessages(
		groups,
		localNodeID,
		func(nodeID uint64, _ []*channelRecentMessageReq) (
			[]*channelRecentMessage,
			error,
		) {
			calls.Add(1)
			return []*channelRecentMessage{
				{ChannelId: fmt.Sprintf("node-%d", nodeID)},
			}, nil
		},
	)
	if err != nil {
		t.Fatalf("request peer recent messages: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("remote call count = %d, want 2", got)
	}

	gotChannels := make(map[string]struct{}, len(results))
	for _, result := range results {
		gotChannels[result.ChannelId] = struct{}{}
	}
	for _, channelID := range []string{"node-2", "node-3"} {
		if _, exists := gotChannels[channelID]; !exists {
			t.Fatalf("missing result for %s", channelID)
		}
	}
	if _, exists := gotChannels["node-1"]; exists {
		t.Fatal("local node must not be requested by peer helper")
	}
}

func TestRequestPeerRecentMessagesCollectsConcurrentErrors(t *testing.T) {
	const peerCount = 64
	groups := make(map[uint64][]*channelRecentMessageReq, peerCount)
	for nodeID := uint64(1); nodeID <= peerCount; nodeID++ {
		groups[nodeID] = []*channelRecentMessageReq{
			{ChannelId: fmt.Sprintf("channel-%d", nodeID)},
		}
	}

	for attempt := 0; attempt < 20; attempt++ {
		results, err := requestPeerRecentMessages(
			groups,
			0,
			func(nodeID uint64, _ []*channelRecentMessageReq) (
				[]*channelRecentMessage,
				error,
			) {
				return nil, fmt.Errorf("node %d: %w", nodeID, errors.New("failed"))
			},
		)
		if err == nil {
			t.Fatal("concurrent peer failures must return an error")
		}
		if results != nil {
			t.Fatalf("results = %#v, want nil when any peer fails", results)
		}
	}
}
