package api

import (
	"encoding/json"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestAppendFoundMessagesKeepsEveryRetryCopy(t *testing.T) {
	results := []wkdb.Message{
		{RecvPacket: wkproto.RecvPacket{MessageID: 1001, ClientMsgNo: "retry-1"}},
		{RecvPacket: wkproto.RecvPacket{MessageID: 1002, ClientMsgNo: "retry-1"}},
	}

	messages := appendFoundMessages(nil, results)

	if len(messages) != 2 {
		t.Fatalf("len(messages) = %d, want 2", len(messages))
	}
	if messages[0].MessageID != 1001 || messages[1].MessageID != 1002 {
		t.Fatalf("message IDs = [%d %d], want [1001 1002]", messages[0].MessageID, messages[1].MessageID)
	}
}

func TestClientMsgNoStoreSearchRequestScopesSelectedSender(t *testing.T) {
	req := newClientMsgNoStoreSearchRequest("group-1", 2, "collision", "target-sender", 64)
	if req.ChannelId != "group-1" || req.ChannelType != 2 || req.ClientMsgNo != "collision" {
		t.Fatalf("unexpected retry lookup identity: %#v", req)
	}
	if req.FromUid != "target-sender" {
		t.Fatalf("from_uid = %q, want selected target sender", req.FromUid)
	}
	if req.Limit != 65 {
		t.Fatalf("limit = %d, want max+1 completeness probe", req.Limit)
	}
	if clientMsgNoQueryProtocolVersion != 2 {
		t.Fatalf("protocol version = %d, want 2", clientMsgNoQueryProtocolVersion)
	}
}

func TestBoundedClientMsgNoCandidateOverflowIsFailClosed(t *testing.T) {
	if !isBoundedClientMsgNoCandidateOverflow(wkdb.ErrMessageSearchCandidateLimit, maxClientMsgNoSearchResults) {
		t.Fatal("bounded candidate overflow was not mapped to a fail-closed More response")
	}
	if isBoundedClientMsgNoCandidateOverflow(wkdb.ErrMessageSearchCandidateLimit, 0) {
		t.Fatal("legacy client_msg_no query was mapped to the v2 overflow contract")
	}
}

func TestLegacyClientMsgNoQueryReturnsOnlyFirstResultPerKey(t *testing.T) {
	req := newLegacyClientMsgNoStoreSearchRequest("group-1", 2, "legacy-key")
	if req.Limit != 1 || req.FromUid != "" {
		t.Fatalf("legacy store request = %#v, want limit 1 without sender scope", req)
	}
	results := []wkdb.Message{
		{RecvPacket: wkproto.RecvPacket{MessageID: 1001, ClientMsgNo: "legacy-key"}},
		{RecvPacket: wkproto.RecvPacket{MessageID: 1002, ClientMsgNo: "legacy-key"}},
	}
	messages := appendLegacyClientMsgNoResult(nil, results)
	if len(messages) != 1 || messages[0].MessageID != 1001 {
		t.Fatalf("legacy messages = %#v, want only the first result", messages)
	}
}

func TestBoundedRetrySearchUsesMaxPlusOneAndDropsOverflowPayloads(t *testing.T) {
	results := make([]wkdb.Message, maxClientMsgNoSearchResults+1)
	for index := range results {
		results[index] = wkdb.Message{RecvPacket: wkproto.RecvPacket{
			MessageID:   int64(index + 1),
			ClientMsgNo: "retry-overflow",
			Payload:     []byte("must not be returned on overflow"),
		}}
	}

	if got := clientMsgNoStoreSearchLimit(maxClientMsgNoSearchResults); got != maxClientMsgNoSearchResults+1 {
		t.Fatalf("store limit = %d, want %d", got, maxClientMsgNoSearchResults+1)
	}
	messages, more := boundedClientMsgNoResults(nil, results, maxClientMsgNoSearchResults)
	if more != 1 {
		t.Fatalf("more = %d, want 1", more)
	}
	if len(messages) != 0 {
		t.Fatalf("overflow response retained %d message payload(s), want 0", len(messages))
	}
}

func TestBoundedRetrySearchKeepsEveryResultWithinLimit(t *testing.T) {
	results := []wkdb.Message{
		{RecvPacket: wkproto.RecvPacket{MessageID: 1001, ClientMsgNo: "retry-1"}},
		{RecvPacket: wkproto.RecvPacket{MessageID: 1002, ClientMsgNo: "retry-1"}},
	}

	messages, more := boundedClientMsgNoResults(nil, results, maxClientMsgNoSearchResults)
	if more != 0 || len(messages) != 2 {
		t.Fatalf("bounded result = len %d more %d, want len 2 more 0", len(messages), more)
	}
}

func TestClientMsgNoQueryProtocolMarkerOnlyOnCompleteBoundedResult(t *testing.T) {
	tests := []struct {
		name           string
		requestedLimit int
		more           int
		want           int
	}{
		{name: "complete bounded query", requestedLimit: maxClientMsgNoSearchResults, want: clientMsgNoQueryProtocolVersion},
		{name: "legacy query", requestedLimit: 0, want: 0},
		{name: "overflow", requestedLimit: maxClientMsgNoSearchResults, more: 1, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := completeClientMsgNoQueryVersion(test.requestedLimit, test.more); got != test.want {
				t.Fatalf("protocol marker = %d, want %d", got, test.want)
			}
		})
	}
}

func TestClientMsgNoQueryProtocolMarkerIsSerializedOnlyForCompleteResult(t *testing.T) {
	complete, err := json.Marshal(syncMessageResp{
		ClientMsgNoQueryVersion: completeClientMsgNoQueryVersion(maxClientMsgNoSearchResults, 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	var completeFields map[string]interface{}
	if json.Unmarshal(complete, &completeFields) != nil || completeFields["client_msg_no_query_version"] != float64(clientMsgNoQueryProtocolVersion) {
		t.Fatalf("complete response protocol marker is missing: %s", complete)
	}

	overflow, err := json.Marshal(syncMessageResp{
		More:                    1,
		ClientMsgNoQueryVersion: completeClientMsgNoQueryVersion(maxClientMsgNoSearchResults, 1),
	})
	if err != nil {
		t.Fatal(err)
	}
	var overflowFields map[string]interface{}
	if json.Unmarshal(overflow, &overflowFields) != nil {
		t.Fatal("decode overflow response")
	}
	if _, exists := overflowFields["client_msg_no_query_version"]; exists {
		t.Fatalf("overflow response claimed completeness: %s", overflow)
	}
}
