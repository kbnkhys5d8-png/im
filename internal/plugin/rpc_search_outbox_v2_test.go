package plugin

import (
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

func TestSearchOutboxRejectsLegacyMutationAndPull(t *testing.T) {
	store := &fakeSearchOutboxStore{}
	rpc := testSearchOutboxRPC(store)
	if _, err := rpc.searchOutboxPull(searchOutboxPullRequest{
		Version: 1, Limit: 1, MaxBytes: 1024,
	}); err == nil {
		t.Fatal("legacy pull bypassed epoch capability")
	}
	if _, err := rpc.searchOutboxAck(searchOutboxAckRequest{
		Version: 1, NodeID: 9,
		Identities: []wkdb.SearchOutboxIdentity{{
			ChannelID: "channel", ChannelType: 2, MessageSeq: 1, MessageID: 11,
		}},
	}); err == nil {
		t.Fatal("legacy ack bypassed epoch fence")
	}
	if store.pullCalls != 0 || store.ackCalls != 0 {
		t.Fatal("legacy protocol reached storage")
	}
}

func TestSearchOutboxCapabilitiesV2(t *testing.T) {
	rpc := testSearchOutboxRPC(&fakeSearchOutboxStore{})
	got, err := rpc.searchOutboxCapabilities(searchOutboxCapabilitiesRequest{Version: 2})
	if err != nil || got.Version != 2 || got.NodeID != 9 || !got.DurableQuarantine || !got.EpochFencing {
		t.Fatalf("capabilities=%+v err=%v", got, err)
	}
	if _, err := rpc.searchOutboxCapabilities(searchOutboxCapabilitiesRequest{Version: 1}); err == nil {
		t.Fatal("legacy capability accepted")
	}
	rpc.searchOutboxReady = func() error { return errors.New("not ready") }
	if _, err := rpc.searchOutboxCapabilities(searchOutboxCapabilitiesRequest{Version: 2}); err == nil {
		t.Fatal("unready capabilities accepted")
	}
}

func validTaskRequest() searchOutboxTaskRequest {
	return searchOutboxTaskRequest{
		Version: 2, NodeID: 9, Reason: "bulk_item_rejected",
		Task: wkdb.SearchOutboxTask{Epoch: 8, Identity: wkdb.SearchOutboxIdentity{
			ChannelID: "channel", ChannelType: 2, MessageSeq: 1, MessageID: 11,
		}},
	}
}

func TestSearchOutboxQuarantineAndRestorePreserveEpoch(t *testing.T) {
	store := &fakeSearchOutboxStore{}
	rpc := testSearchOutboxRPC(store)
	req := validTaskRequest()
	got, err := rpc.searchOutboxQuarantine(req)
	if err != nil || !got.Quarantined || got.NodeID != 9 || got.Version != 2 || store.quarantined != req.Task || store.quarantineReason != req.Reason {
		t.Fatalf("quarantine=%+v err=%v", got, err)
	}
	restored, err := rpc.searchOutboxRestore(req)
	if err != nil || restored.Epoch != 9 || store.restored != req.Task {
		t.Fatalf("restore=%+v err=%v", restored, err)
	}
	store.quarantineErr = wkdb.ErrSearchOutboxStaleEpoch
	if _, err := rpc.searchOutboxQuarantine(req); !errors.Is(err, wkdb.ErrSearchOutboxStaleEpoch) {
		t.Fatalf("stale quarantine=%v", err)
	}
	store.restoreErr = wkdb.ErrSearchOutboxStaleEpoch
	if _, err := rpc.searchOutboxRestore(req); !errors.Is(err, wkdb.ErrSearchOutboxStaleEpoch) {
		t.Fatalf("stale restore=%v", err)
	}
}

func TestSearchOutboxMutationsFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*searchOutboxTaskRequest, *rpc)
	}{
		{"version", func(r *searchOutboxTaskRequest, _ *rpc) { r.Version = 1 }},
		{"node", func(r *searchOutboxTaskRequest, _ *rpc) { r.NodeID = 8 }},
		{"epoch", func(r *searchOutboxTaskRequest, _ *rpc) { r.Task.Epoch = 0 }},
		{"identity", func(r *searchOutboxTaskRequest, _ *rpc) { r.Task.Identity.ChannelID = "" }},
		{"unready", func(_ *searchOutboxTaskRequest, a *rpc) {
			a.searchOutboxReady = func() error { return errors.New("not ready") }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeSearchOutboxStore{}
			a := testSearchOutboxRPC(store)
			req := validTaskRequest()
			tc.change(&req, a)
			if _, err := a.searchOutboxQuarantine(req); err == nil {
				t.Fatal("invalid quarantine accepted")
			}
			if _, err := a.searchOutboxRestore(req); err == nil {
				t.Fatal("invalid restore accepted")
			}
			if store.quarantineCalls != 0 || store.restoreCalls != 0 {
				t.Fatal("invalid mutation reached store")
			}
		})
	}
	store := &fakeSearchOutboxStore{}
	a := testSearchOutboxRPC(store)
	req := validTaskRequest()
	req.Reason = "secret message text"
	if _, err := a.searchOutboxQuarantine(req); err == nil || store.quarantineCalls != 0 {
		t.Fatal("raw reason accepted")
	}
	req = validTaskRequest()
	req.Task.Epoch = 0
	if _, err := a.searchOutboxAck(searchOutboxAckRequest{Version: 2, NodeID: 9, Tasks: []wkdb.SearchOutboxTask{req.Task}}); err == nil || store.ackCalls != 0 {
		t.Fatal("zero epoch ack accepted")
	}
}
