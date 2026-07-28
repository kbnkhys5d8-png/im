package plugin

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/WuKongIM/wkrpc"
	"github.com/panjf2000/gnet/v2"
)

type fakeSearchOutboxStore struct {
	pullResult wkdb.SearchOutboxPullResult
	pullErr    error
	pullCalls  int
	acked      []wkdb.SearchOutboxIdentity
	ackErr     error
	ackCalls   int
}

func (s *fakeSearchOutboxStore) PullSearchOutbox(
	limit int,
	maxBytes uint64,
) (wkdb.SearchOutboxPullResult, error) {
	s.pullCalls++
	return s.pullResult, s.pullErr
}

func (s *fakeSearchOutboxStore) AckSearchOutbox(
	identities []wkdb.SearchOutboxIdentity,
) error {
	s.ackCalls++
	s.acked = append([]wkdb.SearchOutboxIdentity(nil), identities...)
	return s.ackErr
}

func TestSearchOutboxPullReturnsStableAppliedRecords(t *testing.T) {
	message := wkdb.Message{RecvPacket: wkproto.RecvPacket{
		MessageID: 701, MessageSeq: 7, ClientMsgNo: "client-7",
		StreamNo: "stream-7", Timestamp: 1700000007,
		FromUID: "sender", ChannelID: "channel", ChannelType: 2,
		Topic: "topic", Payload: []byte("payload"),
	}}
	identity := wkdb.SearchOutboxIdentity{
		ChannelID: "channel", ChannelType: 2,
		MessageSeq: 7, MessageID: 701,
	}
	store := &fakeSearchOutboxStore{pullResult: wkdb.SearchOutboxPullResult{
		Records: []wkdb.SearchOutboxRecord{{
			Identity: identity, Message: message, AppliedIndex: 9,
		}},
		Pending: 3, OldestCreatedAt: 1699999999, AppliedBlocked: 2,
	}}
	rpc := testSearchOutboxRPC(store)
	request := searchOutboxPullRequest{
		Version: searchOutboxProtocolVersion, Limit: 10, MaxBytes: 1 << 20,
	}

	first, err := rpc.searchOutboxPull(request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := rpc.searchOutboxPull(request)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeat pulls differ: first=%+v second=%+v", first, second)
	}
	if store.pullCalls != 2 ||
		first.Version != searchOutboxProtocolVersion ||
		first.NodeID != 9 ||
		first.Pending != 3 ||
		first.OldestCreatedAt != 1699999999 ||
		first.AppliedBlocked != 2 ||
		len(first.Records) != 1 {
		t.Fatalf("pull response = %+v, calls=%d", first, store.pullCalls)
	}
	record := first.Records[0]
	if record.Identity != identity || record.AppliedMessageSeq != 9 {
		t.Fatalf("record identity/applied = %+v", record)
	}
	got := record.Message
	if got.MessageID != message.MessageID ||
		got.MessageSeq != uint64(message.MessageSeq) ||
		got.ClientMsgNo != message.ClientMsgNo ||
		got.StreamNo != message.StreamNo ||
		got.Timestamp != uint32(message.Timestamp) ||
		got.FromUID != message.FromUID ||
		got.ChannelID != message.ChannelID ||
		got.ChannelType != message.ChannelType ||
		got.Topic != message.Topic ||
		!bytes.Equal(got.Payload, message.Payload) {
		t.Fatalf("mapped message = %+v, want %+v", got, message)
	}
}

func TestSearchOutboxPullRejectsInvalidVersionLimitAndByteBudget(t *testing.T) {
	tests := []struct {
		name string
		req  searchOutboxPullRequest
	}{
		{
			name: "version",
			req: searchOutboxPullRequest{
				Version: searchOutboxProtocolVersion + 1, Limit: 1, MaxBytes: 1,
			},
		},
		{
			name: "zero limit",
			req: searchOutboxPullRequest{
				Version: searchOutboxProtocolVersion, Limit: 0, MaxBytes: 1,
			},
		},
		{
			name: "limit above maximum",
			req: searchOutboxPullRequest{
				Version: searchOutboxProtocolVersion,
				Limit:   wkdb.MaxSearchOutboxPullLimit + 1, MaxBytes: 1,
			},
		},
		{
			name: "zero byte budget",
			req: searchOutboxPullRequest{
				Version: searchOutboxProtocolVersion, Limit: 1, MaxBytes: 0,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeSearchOutboxStore{}
			rpc := testSearchOutboxRPC(store)
			if _, err := rpc.searchOutboxPull(test.req); err == nil {
				t.Fatal("invalid pull request was accepted")
			}
			if store.pullCalls != 0 {
				t.Fatalf("invalid pull reached store %d times", store.pullCalls)
			}
		})
	}
}

func TestSearchOutboxPullFailsClosedWhenRecoveryIsNotReady(t *testing.T) {
	store := &fakeSearchOutboxStore{}
	rpc := testSearchOutboxRPC(store)
	rpc.searchOutboxReady = func() error {
		return errors.New("recovery incomplete")
	}

	if _, err := rpc.searchOutboxPull(searchOutboxPullRequest{
		Version: searchOutboxProtocolVersion, Limit: 1, MaxBytes: 1,
	}); err == nil {
		t.Fatal("runtime-unready pull was accepted")
	}
	if store.pullCalls != 0 {
		t.Fatalf("runtime-unready pull reached store %d times", store.pullCalls)
	}
}

func TestSearchOutboxPullRejectsInvalidNodeTimestampAndAppliedState(t *testing.T) {
	validIdentity := wkdb.SearchOutboxIdentity{
		ChannelID: "channel", ChannelType: 2,
		MessageSeq: 2, MessageID: 22,
	}
	validMessage := wkdb.Message{RecvPacket: wkproto.RecvPacket{
		MessageID: 22, MessageSeq: 2, Timestamp: 100,
		ChannelID: "channel", ChannelType: 2,
	}}
	tests := []struct {
		name      string
		nodeID    uint64
		record    wkdb.SearchOutboxRecord
		storeCall bool
	}{
		{
			name:   "zero local node id",
			nodeID: 0,
		},
		{
			name:   "zero timestamp",
			nodeID: 9,
			record: wkdb.SearchOutboxRecord{
				Identity: validIdentity,
				Message: wkdb.Message{RecvPacket: wkproto.RecvPacket{
					MessageID: 22, MessageSeq: 2, Timestamp: 0,
					ChannelID: "channel", ChannelType: 2,
				}},
				AppliedIndex: 2,
			},
			storeCall: true,
		},
		{
			name:   "negative timestamp",
			nodeID: 9,
			record: wkdb.SearchOutboxRecord{
				Identity: validIdentity,
				Message: wkdb.Message{RecvPacket: wkproto.RecvPacket{
					MessageID: 22, MessageSeq: 2, Timestamp: -1,
					ChannelID: "channel", ChannelType: 2,
				}},
				AppliedIndex: 2,
			},
			storeCall: true,
		},
		{
			name:   "applied index below message sequence",
			nodeID: 9,
			record: wkdb.SearchOutboxRecord{
				Identity:     validIdentity,
				Message:      validMessage,
				AppliedIndex: 1,
			},
			storeCall: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeSearchOutboxStore{}
			if test.storeCall {
				store.pullResult.Records = []wkdb.SearchOutboxRecord{test.record}
			}
			rpc := testSearchOutboxRPC(store)
			rpc.searchOutboxNodeID = func() uint64 { return test.nodeID }

			if _, err := rpc.searchOutboxPull(searchOutboxPullRequest{
				Version:  searchOutboxProtocolVersion,
				Limit:    1,
				MaxBytes: 1 << 20,
			}); err == nil {
				t.Fatal("invalid outbox record or node was accepted")
			}
			wantCalls := 0
			if test.storeCall {
				wantCalls = 1
			}
			if store.pullCalls != wantCalls {
				t.Fatalf("store calls = %d, want %d", store.pullCalls, wantCalls)
			}
		})
	}
}

func TestSearchOutboxPullDoesNotUseSearchSourceStore(t *testing.T) {
	store := &fakeSearchOutboxStore{pullResult: wkdb.SearchOutboxPullResult{
		Records: make([]wkdb.SearchOutboxRecord, 0),
	}}
	rpc := testSearchOutboxRPC(store)
	rpc.searchSourceStore = panicSearchSourceStore{}

	if _, err := rpc.searchOutboxPull(searchOutboxPullRequest{
		Version: searchOutboxProtocolVersion, Limit: 1, MaxBytes: 1,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestSearchOutboxAckPassesOnlyExactIdentities(t *testing.T) {
	store := &fakeSearchOutboxStore{}
	rpc := testSearchOutboxRPC(store)
	identities := []wkdb.SearchOutboxIdentity{
		{ChannelID: "a", ChannelType: 2, MessageSeq: 1, MessageID: 11},
		{ChannelID: "b", ChannelType: 2, MessageSeq: 2, MessageID: 22},
	}

	response, err := rpc.searchOutboxAck(searchOutboxAckRequest{
		Version: searchOutboxProtocolVersion,
		NodeID:  9, Identities: identities,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(store.acked, identities) {
		t.Fatalf("acked identities = %+v, want %+v", store.acked, identities)
	}
	if response.Version != searchOutboxProtocolVersion ||
		response.NodeID != 9 ||
		response.Acknowledged != len(identities) {
		t.Fatalf("ack response = %+v", response)
	}
}

func TestSearchOutboxAckRepeatIsSuccessful(t *testing.T) {
	store := &fakeSearchOutboxStore{}
	rpc := testSearchOutboxRPC(store)
	request := searchOutboxAckRequest{
		Version: searchOutboxProtocolVersion,
		NodeID:  9,
		Identities: []wkdb.SearchOutboxIdentity{{
			ChannelID: "channel", ChannelType: 2,
			MessageSeq: 1, MessageID: 11,
		}},
	}
	for attempt := 0; attempt < 2; attempt++ {
		if _, err := rpc.searchOutboxAck(request); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	if store.ackCalls != 2 {
		t.Fatalf("ack calls = %d, want 2", store.ackCalls)
	}
}

func TestSearchOutboxAckRejectsInvalidVersionNodeAndIdentities(t *testing.T) {
	valid := wkdb.SearchOutboxIdentity{
		ChannelID: "channel", ChannelType: 2,
		MessageSeq: 1, MessageID: 11,
	}
	tooMany := make([]wkdb.SearchOutboxIdentity, searchOutboxMaxAckCount+1)
	for index := range tooMany {
		tooMany[index] = wkdb.SearchOutboxIdentity{
			ChannelID: "channel", ChannelType: 2,
			MessageSeq: uint64(index + 1), MessageID: int64(index + 1),
		}
	}
	tests := []struct {
		name      string
		req       searchOutboxAckRequest
		localNode func() uint64
	}{
		{
			name: "version",
			req: searchOutboxAckRequest{
				Version: searchOutboxProtocolVersion + 1,
				NodeID:  9, Identities: []wkdb.SearchOutboxIdentity{valid},
			},
		},
		{
			name: "zero node id",
			req: searchOutboxAckRequest{
				Version: searchOutboxProtocolVersion,
				NodeID:  0, Identities: []wkdb.SearchOutboxIdentity{valid},
			},
		},
		{
			name: "different node id",
			req: searchOutboxAckRequest{
				Version: searchOutboxProtocolVersion,
				NodeID:  8, Identities: []wkdb.SearchOutboxIdentity{valid},
			},
		},
		{
			name: "zero local node id",
			req: searchOutboxAckRequest{
				Version: searchOutboxProtocolVersion,
				NodeID:  9, Identities: []wkdb.SearchOutboxIdentity{valid},
			},
			localNode: func() uint64 { return 0 },
		},
		{
			name: "empty identities",
			req: searchOutboxAckRequest{
				Version: searchOutboxProtocolVersion,
				NodeID:  9,
			},
		},
		{
			name: "too many identities",
			req: searchOutboxAckRequest{
				Version: searchOutboxProtocolVersion,
				NodeID:  9, Identities: tooMany,
			},
		},
		{
			name: "duplicate identity",
			req: searchOutboxAckRequest{
				Version: searchOutboxProtocolVersion,
				NodeID:  9, Identities: []wkdb.SearchOutboxIdentity{valid, valid},
			},
		},
		{
			name: "invalid identity",
			req: searchOutboxAckRequest{
				Version: searchOutboxProtocolVersion,
				NodeID:  9,
				Identities: []wkdb.SearchOutboxIdentity{{
					ChannelID: "", ChannelType: 2,
					MessageSeq: 1, MessageID: 11,
				}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeSearchOutboxStore{}
			rpc := testSearchOutboxRPC(store)
			if test.localNode != nil {
				rpc.searchOutboxNodeID = test.localNode
			}
			if _, err := rpc.searchOutboxAck(test.req); err == nil {
				t.Fatal("invalid ack request was accepted")
			}
			if store.ackCalls != 0 {
				t.Fatalf("invalid ack reached store %d times", store.ackCalls)
			}
		})
	}
}

func TestSearchOutboxRoutesRequireManagedLocalSearchPlugin(t *testing.T) {
	tests := []struct {
		name string
		auth *localPluginAuthorizer
	}{
		{name: "missing authorizer"},
		{
			name: "unmanaged process",
			auth: newLocalPluginAuthorizerWithBackend(
				&fakeLocalAuthBackend{},
				501,
			),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn := &searchOutboxUnauthorizedConn{fd: 10}
			context := wkrpc.NewContext(conn)
			called := false
			rpc := &rpc{
				s: &Server{searchAuth: test.auth},
				searchOutboxReady: func() error {
					return nil
				},
			}
			rpc.requireSearchOutboxAuthorization(func(*wkrpc.Context) {
				called = true
			})(context)

			if called {
				t.Fatal("unauthorized local process reached outbox handler")
			}
			if conn.writeCalls != 1 {
				t.Fatalf("error responses = %d, want 1", conn.writeCalls)
			}
		})
	}
}

func testSearchOutboxRPC(store *fakeSearchOutboxStore) *rpc {
	return &rpc{
		searchOutboxStore:  store,
		searchOutboxReady:  func() error { return nil },
		searchOutboxNodeID: func() uint64 { return 9 },
	}
}

type panicSearchSourceStore struct{}

func (panicSearchSourceStore) GetChannelClusterConfigs(uint64, int) ([]wkdb.ChannelClusterConfig, error) {
	panic("search source channel inventory was used")
}

func (panicSearchSourceStore) GetChannelClusterConfigRevision() uint64 {
	panic("search source revision was used")
}

func (panicSearchSourceStore) GetAppliedMsgSeq(string, uint8) (uint64, error) {
	panic("search source applied index was used")
}

func (panicSearchSourceStore) GetLastMsgSeq(string, uint8) (uint64, error) {
	panic("search source physical tail was used")
}

func (panicSearchSourceStore) LoadNextRangeSearchSourceMessages(
	string,
	uint8,
	uint64,
	uint64,
	int,
	uint64,
) ([]wkdb.SearchSourceMessage, error) {
	panic("search source message load was used")
}

type searchOutboxUnauthorizedConn struct {
	gnet.Conn
	fd         int
	writeCalls int
}

func (c *searchOutboxUnauthorizedConn) Context() any { return nil }

func (c *searchOutboxUnauthorizedConn) Fd() int { return c.fd }

func (c *searchOutboxUnauthorizedConn) AsyncWrite(
	_ []byte,
	callback gnet.AsyncCallback,
) error {
	c.writeCalls++
	if callback != nil {
		return callback(c, nil)
	}
	return nil
}
