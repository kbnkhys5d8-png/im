package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestSearchSourceChannelsChecksMalformedRosterBeforeStore(t *testing.T) {
	store := &fakeSearchSourceStore{}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceRoster = func() ([]uint64, error) { return []uint64{2}, nil }

	_, err := rpc.searchSourceChannels(searchSourceChannelPageRequest{Version: 5, Limit: 10})
	if !errors.Is(err, errSearchSourceRoster) {
		t.Fatalf("error = %v, want roster error", err)
	}
	if len(store.calls) != 0 {
		t.Fatalf("store was read before roster failed: %v", store.calls)
	}
}

func TestSearchSourceMessagesChecksMalformedRosterBeforeAuthorityOrStore(t *testing.T) {
	store := &fakeSearchSourceStore{}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceRoster = func() ([]uint64, error) { return []uint64{2}, nil }
	authorityCalls := 0
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) {
		authorityCalls++
		return validSearchSourceConfig(), nil
	}
	_, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3, ExpectedClusterNodeIDs: []uint64{1}, ExpectedConfigRevision: 2,
		NextSeq: 1, Limit: 10,
	})
	if !errors.Is(err, errSearchSourceRoster) {
		t.Fatalf("error = %v, want roster error", err)
	}
	if authorityCalls != 0 || len(store.calls) != 0 {
		t.Fatalf("authority/store read before roster failed: authority=%d store=%v", authorityCalls, store.calls)
	}
}

func TestSearchSourceChannelsAcceptsStableThreeNodeRosterAndReturnsOnlyLocalLeaders(t *testing.T) {
	remote := validThreeNodeSearchSourceConfig(9, "remote", 1)
	local := validThreeNodeSearchSourceConfig(10, "local", 2)
	store := &fakeSearchSourceStore{
		configs:  []wkdb.ChannelClusterConfig{remote, local},
		applied:  3,
		physical: 3,
	}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceNodeID = func() uint64 { return 2 }
	rpc.searchSourceRoster = func() ([]uint64, error) { return []uint64{3, 1, 2}, nil }
	rpc.searchSourceAuthority = func(channelID string, _ uint8) (wkdb.ChannelClusterConfig, error) {
		if channelID == local.ChannelId {
			return local, nil
		}
		return remote, nil
	}

	resp, err := rpc.searchSourceChannels(searchSourceChannelPageRequest{Version: 5, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if resp.NodeID != 2 || !reflect.DeepEqual(resp.ClusterNodeIDs, []uint64{1, 2, 3}) {
		t.Fatalf("topology response = %+v", resp)
	}
	if resp.ScannedTo != local.Id || len(resp.Channels) != 1 || resp.Channels[0].ChannelID != local.ChannelId {
		t.Fatalf("local inventory response = %+v", resp)
	}
}

func TestSearchSourceChannelsDoesNotCallRemoteAuthorityForFullPage(t *testing.T) {
	configs := make([]wkdb.ChannelClusterConfig, 500)
	for index := range configs {
		configs[index] = validThreeNodeSearchSourceConfig(uint64(index+1), fmt.Sprintf("remote-%d", index), 1)
	}
	store := &fakeSearchSourceStore{configs: configs}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceNodeID = func() uint64 { return 2 }
	rpc.searchSourceRoster = func() ([]uint64, error) { return []uint64{1, 2, 3}, nil }
	authorityCalls := 0
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) {
		authorityCalls++
		return wkdb.ChannelClusterConfig{}, errors.New("remote authority must not be called by inventory")
	}

	resp, err := rpc.searchSourceChannels(searchSourceChannelPageRequest{Version: 5, Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	if authorityCalls != 0 || resp.ScannedTo != 500 || len(resp.Channels) != 0 {
		t.Fatalf("authority=%d scanned_to=%d channels=%d", authorityCalls, resp.ScannedTo, len(resp.Channels))
	}
}

func TestSearchSourceMessagesAcceptsStableThreeNodeRosterForLocalLeader(t *testing.T) {
	cfg := validThreeNodeSearchSourceConfig(9, "channel", 2)
	store := &fakeSearchSourceStore{}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceNodeID = func() uint64 { return 2 }
	rpc.searchSourceRoster = func() ([]uint64, error) { return []uint64{1, 2, 3}, nil }
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }

	resp, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 2, ExpectedTerm: 2, ExpectedConfigVersion: 3, ExpectedClusterNodeIDs: []uint64{1, 2, 3}, ExpectedConfigRevision: 2,
		NextSeq: 1, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.NodeID != 2 || !reflect.DeepEqual(resp.ClusterNodeIDs, []uint64{1, 2, 3}) || !resp.CaughtUp {
		t.Fatalf("message topology response = %+v", resp)
	}
}

func TestSearchSourceMessagesNotOwnerRechecksRosterBeforeReturning(t *testing.T) {
	cfg := validThreeNodeSearchSourceConfig(9, "channel", 1)
	store := &fakeSearchSourceStore{}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceNodeID = func() uint64 { return 2 }
	rosterCalls := 0
	rpc.searchSourceRoster = func() ([]uint64, error) {
		rosterCalls++
		if rosterCalls == 1 {
			return []uint64{1, 2, 3}, nil
		}
		return []uint64{1, 2, 4}, nil
	}
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }

	_, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 2, ExpectedTerm: 2, ExpectedConfigVersion: 3,
		ExpectedClusterNodeIDs: []uint64{1, 2, 3}, ExpectedConfigRevision: 2,
		NextSeq: 1, Limit: 10,
	})
	if !errors.Is(err, errSearchSourceRoster) {
		t.Fatalf("not-owner roster change error = %v, want roster fence", err)
	}
}

func TestSearchSourceMessagesLateNotOwnerRechecksRosterBeforeReturning(t *testing.T) {
	before := validThreeNodeSearchSourceConfig(9, "channel", 2)
	after := before
	after.LeaderId = 1
	after.Term++
	after.ConfVersion++
	store := &fakeSearchSourceStore{
		applied:  1,
		physical: 1,
		messages: []wkdb.SearchSourceMessage{{Message: wkdb.Message{RecvPacket: wkproto.RecvPacket{
			MessageSeq:  1,
			MessageID:   1,
			ChannelID:   "channel",
			ChannelType: 2,
		}}}},
	}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceNodeID = func() uint64 { return 2 }
	rosterCalls := 0
	rpc.searchSourceRoster = func() ([]uint64, error) {
		rosterCalls++
		if rosterCalls == 1 {
			return []uint64{1, 2, 3}, nil
		}
		return []uint64{1, 2, 4}, nil
	}
	authorityCalls := 0
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) {
		authorityCalls++
		if authorityCalls == 1 {
			return before, nil
		}
		return after, nil
	}

	_, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 2, ExpectedTerm: 2, ExpectedConfigVersion: 3,
		ExpectedClusterNodeIDs: []uint64{1, 2, 3}, ExpectedConfigRevision: 2,
		NextSeq: 1, Limit: 10,
	})
	if !errors.Is(err, errSearchSourceRoster) {
		t.Fatalf("late not-owner roster change error = %v, want roster fence", err)
	}
}

func TestSearchSourceChannelsRejectsRosterChangeDuringRead(t *testing.T) {
	cfg := validThreeNodeSearchSourceConfig(9, "channel", 1)
	store := &fakeSearchSourceStore{configs: []wkdb.ChannelClusterConfig{cfg}}
	rpc := testSearchSourceRPC(store)
	rosterCalls := 0
	rpc.searchSourceRoster = func() ([]uint64, error) {
		rosterCalls++
		if rosterCalls == 1 {
			return []uint64{1, 2, 3}, nil
		}
		return []uint64{1, 2, 4}, nil
	}
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }

	if _, err := rpc.searchSourceChannels(searchSourceChannelPageRequest{Version: 5, Limit: 10}); !errors.Is(err, errSearchSourceRoster) {
		t.Fatalf("error = %v, want roster fence", err)
	}
}

func TestSearchSourceChannelsRejectsConfigRevisionChangeDuringRead(t *testing.T) {
	cfg := validThreeNodeSearchSourceConfig(9, "channel", 1)
	store := &fakeSearchSourceStore{
		configs:         []wkdb.ChannelClusterConfig{cfg},
		configRevisions: []uint64{2, 4},
	}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceRoster = func() ([]uint64, error) { return []uint64{1, 2, 3}, nil }
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }

	if _, err := rpc.searchSourceChannels(searchSourceChannelPageRequest{Version: 5, Limit: 10}); !errors.Is(err, errSearchSourceFence) {
		t.Fatalf("error = %v, want config revision fence", err)
	}
}

func TestSearchSourceBootstrapFailureDisablesBothSourceMethodsBeforeDBReads(t *testing.T) {
	store := &fakeSearchSourceStore{}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceReady = func() error { return errSearchSourceUnavailable }

	if _, err := rpc.searchSourceChannels(searchSourceChannelPageRequest{Version: 5, Limit: 10}); !errors.Is(err, errSearchSourceUnavailable) {
		t.Fatalf("channel source error = %v, want unavailable", err)
	}
	if _, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3, ExpectedClusterNodeIDs: []uint64{1}, ExpectedConfigRevision: 2,
		NextSeq: 1, Limit: 10,
	}); !errors.Is(err, errSearchSourceUnavailable) {
		t.Fatalf("message source error = %v, want unavailable", err)
	}
	if len(store.calls) != 0 {
		t.Fatalf("disabled source touched DB: %v", store.calls)
	}
}

func TestSearchSourceChannelsReturnsV5TopologyAndWatermarks(t *testing.T) {
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceStore{
		configs:  []wkdb.ChannelClusterConfig{cfg},
		applied:  3,
		physical: 5,
	}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }

	resp, err := rpc.searchSourceChannels(searchSourceChannelPageRequest{Version: 5, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Version != 5 || resp.NodeID != 1 || !reflect.DeepEqual(resp.ClusterNodeIDs, []uint64{1}) {
		t.Fatalf("topology response = %+v", resp)
	}
	if resp.ScannedTo != cfg.Id || len(resp.Channels) != 1 {
		t.Fatalf("inventory response = %+v", resp)
	}
	channel := resp.Channels[0]
	if channel.LastMessageSeq != 3 || channel.AppliedMessageSeq != 3 || channel.PhysicalMessageSeq != 5 {
		t.Fatalf("watermarks = %+v", channel)
	}
	if !channel.ApplyPending || channel.OfflineBootstrapRequired {
		t.Fatalf("pending/bootstrap markers = %+v", channel)
	}
}

func TestSearchSourceMessagesV5ExplicitlyEncodesSearchPolicyFields(t *testing.T) {
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceStore{
		applied:  1,
		physical: 1,
		messages: []wkdb.SearchSourceMessage{{Message: wkdb.Message{RecvPacket: wkproto.RecvPacket{
			MessageID: 7, MessageSeq: 1, ClientMsgNo: "client", ChannelID: "channel", ChannelType: 2,
			Expire: 0, Setting: 0, Framer: wkproto.Framer{NoPersist: false}, Payload: []byte("payload"),
		}}}},
	}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }

	resp, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3, ExpectedClusterNodeIDs: []uint64{1}, ExpectedConfigRevision: 2,
		NextSeq: 1, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.NextSeq != 2 || !resp.CaughtUp || len(resp.Messages) != 1 {
		t.Fatalf("message response = %+v", resp)
	}
	encoded, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	message := decoded["messages"].([]any)[0].(map[string]any)
	for _, field := range []string{"expire", "setting", "no_persist", "payload_omitted"} {
		value, ok := message[field]
		if !ok || value == nil {
			t.Fatalf("field %q must be explicit and non-null: %s", field, encoded)
		}
	}
}

func TestSearchSourceMessagesRejectsPhysicallyStoredNoPersistMessage(t *testing.T) {
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceStore{
		applied:  1,
		physical: 1,
		messages: []wkdb.SearchSourceMessage{{Message: wkdb.Message{RecvPacket: wkproto.RecvPacket{
			Framer: wkproto.Framer{NoPersist: true}, MessageSeq: 1,
			ChannelID: "channel", ChannelType: 2,
		}}}},
	}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }

	_, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3, ExpectedClusterNodeIDs: []uint64{1}, ExpectedConfigRevision: 2,
		NextSeq: 1, Limit: 10,
	})
	if err == nil {
		t.Fatal("physically stored no-persist message was exposed to search")
	}
}

func TestSearchSourceMessagesNeverPromotesPhysicalTailToApplied(t *testing.T) {
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceStore{applied: 0, physical: 5}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }

	resp, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3, ExpectedClusterNodeIDs: []uint64{1}, ExpectedConfigRevision: 2,
		NextSeq: 1, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.AppliedMessageSeq != 0 || resp.LastMessageSeq != 0 || resp.PhysicalMessageSeq != 5 {
		t.Fatalf("physical tail was promoted: %+v", resp)
	}
	if !resp.OfflineBootstrapRequired || resp.CaughtUp {
		t.Fatalf("offline bootstrap was not surfaced: %+v", resp)
	}
	if store.loadCalls != 0 {
		t.Fatalf("read %d pages beyond applied watermark", store.loadCalls)
	}
}

func TestSearchSourceMessagesFencesApplyPendingResponse(t *testing.T) {
	cfg := validSearchSourceConfig()
	changed := cfg
	changed.ConfVersion++
	store := &fakeSearchSourceStore{applied: 0, physical: 5}
	rpc := testSearchSourceRPC(store)
	authorityCalls := 0
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) {
		authorityCalls++
		if authorityCalls == 1 {
			return cfg, nil
		}
		return changed, nil
	}
	resp, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3, ExpectedClusterNodeIDs: []uint64{1}, ExpectedConfigRevision: 2,
		NextSeq: 1, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if authorityCalls != 2 || !resp.NotOwner || !resp.Retryable {
		t.Fatalf("pending response was not generation-fenced: calls=%d resp=%+v", authorityCalls, resp)
	}
	if resp.LastMessageSeq != 0 || resp.AppliedMessageSeq != 0 || resp.PhysicalMessageSeq != 0 ||
		resp.ApplyPending || resp.OfflineBootstrapRequired || resp.CaughtUp {
		t.Fatalf("not-owner response retained stale source state: %+v", resp)
	}
}

func TestSearchSourceMessagesFailsClosedWhenGenerationChanges(t *testing.T) {
	cfg := validSearchSourceConfig()
	changed := cfg
	changed.Term++
	store := &fakeSearchSourceStore{applied: 1, physical: 1}
	rpc := testSearchSourceRPC(store)
	authorityCalls := 0
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) {
		authorityCalls++
		if authorityCalls == 1 {
			return cfg, nil
		}
		return changed, nil
	}

	resp, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3, ExpectedClusterNodeIDs: []uint64{1}, ExpectedConfigRevision: 2,
		NextSeq: 2, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.NotOwner || !resp.Retryable || len(resp.Messages) != 0 {
		t.Fatalf("generation change did not fail closed: %+v", resp)
	}
}

func TestOfflineSearchBootstrapNoMarkerClosesFirstStartupWindowWithoutDBReads(t *testing.T) {
	store := &fakeSearchSourceBootstrapStore{}
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	err := applySearchSourceOfflineBootstrapMarker(
		markerPath, 1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return validSearchSourceConfig(), nil },
		store,
	)
	if !errors.Is(err, errSearchSourceBootstrapRequired) {
		t.Fatalf("no-marker error = %v, want explicit bootstrap required", err)
	}
	if store.reads != 0 || store.updates != 0 {
		t.Fatalf("no-marker bootstrap read=%d update=%d", store.reads, store.updates)
	}
	if _, err := os.Stat(markerPath + ".window-closed"); err != nil {
		t.Fatalf("first-start bootstrap window was not closed: %v", err)
	}
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":1}`)
	if err := applySearchSourceOfflineBootstrapMarker(markerPath, 1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return validSearchSourceConfig(), nil },
		store,
	); err == nil {
		t.Fatal("marker created after the first new-version startup was accepted")
	}
}

func TestOfflineSearchBootstrapConsumesMarkerAndAdvancesOnlyZeroWatermark(t *testing.T) {
	dir := t.TempDir()
	markerPath := filepath.Join(dir, SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":1}`)
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceBootstrapStore{configs: []wkdb.ChannelClusterConfig{cfg}, applied: 0, physical: 2}
	authority := func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }
	roster := func() ([]uint64, error) { return []uint64{1}, nil }
	if err := applySearchSourceOfflineBootstrapMarker(markerPath, 1, roster, authority, store); err != nil {
		t.Fatal(err)
	}
	if store.applied != 2 || store.updates != 1 {
		t.Fatalf("applied=%d updates=%d, want 2/1", store.applied, store.updates)
	}
	if len(store.limits) != 1 || store.limits[0] != searchSourceBootstrapPageSize {
		t.Fatalf("bootstrap page limits = %v", store.limits)
	}
	if _, err := os.Stat(markerPath + ".consumed"); err != nil {
		t.Fatalf("consumed marker missing: %v", err)
	}
	if err := applySearchSourceOfflineBootstrapMarker(markerPath, 1, roster, authority, store); err != nil {
		t.Fatal(err)
	}
	if store.updates != 1 {
		t.Fatalf("consumed marker ran twice: updates=%d", store.updates)
	}
}

func TestOfflineSearchBootstrapInitializesLocalReplicaOnThreeNodeRoster(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":2}`)
	cfg := validThreeNodeSearchSourceConfig(9, "channel", 1)
	store := &fakeSearchSourceBootstrapStore{
		configs:  []wkdb.ChannelClusterConfig{cfg},
		applied:  0,
		physical: 4,
	}

	err := applySearchSourceOfflineBootstrapMarker(
		markerPath,
		2,
		func() ([]uint64, error) { return []uint64{3, 1, 2}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil },
		store,
	)
	if err != nil {
		t.Fatal(err)
	}
	if store.applied != 4 || store.updates != 1 {
		t.Fatalf("three-node replica bootstrap applied=%d updates=%d, want 4/1", store.applied, store.updates)
	}
}

func TestConsumedOfflineSearchBootstrapRecoversAsyncObserverCrashWindow(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath+".consumed", `{"version":1,"node_id":1}`)
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceBootstrapStore{configs: []wkdb.ChannelClusterConfig{cfg}, applied: 0, physical: 3}
	if err := applySearchSourceOfflineBootstrapMarker(markerPath, 1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil },
		store,
	); err != nil {
		t.Fatal(err)
	}
	if store.applied != 3 || store.updates != 1 {
		t.Fatalf("consumed crash recovery applied=%d updates=%d, want 3/1", store.applied, store.updates)
	}
}

func TestWindowClosedOfflineSearchBootstrapNeverReconcilesPhysicalTail(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath+".window-closed", `{"version":1,"node_id":1}`)
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceBootstrapStore{configs: []wkdb.ChannelClusterConfig{cfg}, physical: 3}
	err := applySearchSourceOfflineBootstrapMarker(markerPath, 1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil },
		store,
	)
	if !errors.Is(err, errSearchSourceBootstrapRequired) {
		t.Fatalf("window-closed error = %v, want explicit bootstrap required", err)
	}
	if store.reads != 0 || store.updates != 0 {
		t.Fatalf("window-closed state reconciled old data: reads=%d updates=%d", store.reads, store.updates)
	}
}

func TestWindowClosedOfflineSearchBootstrapConsumesExplicitRecoveryAuthorization(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath+".window-closed", `{"version":1,"node_id":2}`)
	writeSearchSourceBootstrapMarker(t, markerPath+".recovery-authorized", `{"version":1,"node_id":2}`)
	cfg := validThreeNodeSearchSourceConfig(9, "channel", 1)
	store := &fakeSearchSourceBootstrapStore{
		configs:  []wkdb.ChannelClusterConfig{cfg},
		physical: 4,
	}
	roster := func() ([]uint64, error) { return []uint64{3, 1, 2}, nil }
	authority := func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil }

	if err := applySearchSourceOfflineBootstrapMarker(markerPath, 2, roster, authority, store); err != nil {
		t.Fatal(err)
	}
	if store.applied != 4 || store.updates != 1 {
		t.Fatalf("recovery applied=%d updates=%d, want 4/1", store.applied, store.updates)
	}
	if _, err := os.Stat(markerPath + ".window-closed"); err != nil {
		t.Fatalf("recovery removed the original closed-window evidence: %v", err)
	}
	if _, err := os.Stat(markerPath + ".recovery-consumed"); err != nil {
		t.Fatalf("recovery consumed marker missing: %v", err)
	}
	if _, err := os.Stat(markerPath + ".recovery-authorized"); !os.IsNotExist(err) {
		t.Fatalf("recovery authorization was not consumed: %v", err)
	}
	if err := applySearchSourceOfflineBootstrapMarker(markerPath, 2, roster, authority, store); err != nil {
		t.Fatalf("consumed recovery did not reconcile idempotently: %v", err)
	}
	if store.updates != 1 {
		t.Fatalf("consumed recovery rewrote watermarks: updates=%d", store.updates)
	}
}

func TestOfflineSearchBootstrapRejectsRecoveryAuthorizationWithoutClosedWindow(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath+".recovery-authorized", `{"version":1,"node_id":1}`)
	store := &fakeSearchSourceBootstrapStore{}
	err := applySearchSourceOfflineBootstrapMarker(
		markerPath,
		1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return validSearchSourceConfig(), nil },
		store,
	)
	if err == nil {
		t.Fatal("recovery authorization without a closed window was accepted")
	}
	if store.reads != 0 || store.updates != 0 {
		t.Fatalf("invalid recovery authorization read=%d update=%d", store.reads, store.updates)
	}
	if _, err := os.Stat(markerPath + ".recovery-authorized"); err != nil {
		t.Fatalf("invalid recovery authorization was mutated: %v", err)
	}
	if _, err := os.Stat(markerPath + ".window-closed"); !os.IsNotExist(err) {
		t.Fatalf("invalid recovery authorization changed the closed-window state: %v", err)
	}
}

func TestWindowClosedOfflineSearchRecoveryKeepsApplyingMarkerOnPartialWatermark(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath+".window-closed", `{"version":1,"node_id":1}`)
	writeSearchSourceBootstrapMarker(t, markerPath+".recovery-authorized", `{"version":1,"node_id":1}`)
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceBootstrapStore{
		configs:  []wkdb.ChannelClusterConfig{cfg},
		applied:  1,
		physical: 5,
	}
	err := applySearchSourceOfflineBootstrapMarker(
		markerPath,
		1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil },
		store,
	)
	if err == nil {
		t.Fatal("partial watermark unexpectedly completed closed-window recovery")
	}
	if store.applied != 1 || store.updates != 0 {
		t.Fatalf("partial recovery watermark changed: applied=%d updates=%d", store.applied, store.updates)
	}
	if _, err := os.Stat(markerPath + ".recovery-applying"); err != nil {
		t.Fatalf("failed recovery did not retain its applying marker: %v", err)
	}
	if _, err := os.Stat(markerPath + ".window-closed"); err != nil {
		t.Fatalf("failed recovery removed the original closed-window evidence: %v", err)
	}
}

func TestOfflineSearchBootstrapResumesApplyingMarkerIdempotently(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":1}`)
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceBootstrapStore{configs: []wkdb.ChannelClusterConfig{cfg}, physical: 2}
	authorityCalls := 0
	authorityFailsAfterUpdate := func(string, uint8) (wkdb.ChannelClusterConfig, error) {
		authorityCalls++
		if authorityCalls == 2 {
			return wkdb.EmptyChannelClusterConfig, errors.New("injected post-update fence failure")
		}
		return cfg, nil
	}
	roster := func() ([]uint64, error) { return []uint64{1}, nil }
	if err := applySearchSourceOfflineBootstrapMarker(markerPath, 1, roster, authorityFailsAfterUpdate, store); err == nil {
		t.Fatal("injected bootstrap interruption did not fail")
	}
	if store.applied != 2 || store.updates != 1 {
		t.Fatalf("partial bootstrap state applied=%d updates=%d", store.applied, store.updates)
	}
	if _, err := os.Stat(markerPath + ".applying"); err != nil {
		t.Fatalf("applying marker missing after interruption: %v", err)
	}
	if err := applySearchSourceOfflineBootstrapMarker(markerPath, 1, roster, func(string, uint8) (wkdb.ChannelClusterConfig, error) {
		return cfg, nil
	}, store); err != nil {
		t.Fatalf("resume bootstrap: %v", err)
	}
	if store.updates != 1 {
		t.Fatalf("resume rewrote already initialized watermark: updates=%d", store.updates)
	}
	if _, err := os.Stat(markerPath + ".consumed"); err != nil {
		t.Fatalf("resumed marker was not consumed: %v", err)
	}
}

func TestOfflineSearchBootstrapCancellationLeavesApplyingMarkerForRestart(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":1}`)
	cfg := validSearchSourceConfig()
	ctx, cancel := context.WithCancel(context.Background())
	store := &fakeSearchSourceBootstrapStore{configs: []wkdb.ChannelClusterConfig{cfg}}
	store.onGetConfigs = cancel
	err := applySearchSourceOfflineBootstrapMarkerContext(ctx, markerPath, 1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil },
		store,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if _, err := os.Stat(markerPath + ".applying"); err != nil {
		t.Fatalf("canceled bootstrap did not retain applying marker: %v", err)
	}
	if _, err := os.Stat(markerPath + ".consumed"); !os.IsNotExist(err) {
		t.Fatalf("canceled bootstrap was consumed: %v", err)
	}
}

func TestOfflineSearchBootstrapDoesNotPromotePartialAppliedTail(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":1}`)
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceBootstrapStore{configs: []wkdb.ChannelClusterConfig{cfg}, applied: 1, physical: 5}
	err := applySearchSourceOfflineBootstrapMarker(markerPath, 1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil },
		store,
	)
	if err == nil {
		t.Fatal("partial applied tail unexpectedly completed bootstrap")
	}
	if store.applied != 1 || store.updates != 0 {
		t.Fatalf("partial applied tail was promoted: applied=%d updates=%d", store.applied, store.updates)
	}
	if _, err := os.Stat(markerPath + ".applying"); err != nil {
		t.Fatalf("partial state did not retain applying marker: %v", err)
	}
	if _, err := os.Stat(markerPath + ".consumed"); !os.IsNotExist(err) {
		t.Fatalf("partial state was incorrectly marked consumed: %v", err)
	}
}

func TestOfflineSearchBootstrapIgnoresMessagesWithoutChannelConfig(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":1}`)
	store := &fakeSearchSourceBootstrapStore{physical: 8}
	if err := applySearchSourceOfflineBootstrapMarker(markerPath, 1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) {
			t.Fatal("authority called for message data without a channel config")
			return wkdb.EmptyChannelClusterConfig, nil
		},
		store,
	); err != nil {
		t.Fatal(err)
	}
	if store.updates != 0 {
		t.Fatalf("orphan message tail was bootstrapped: updates=%d", store.updates)
	}
}

func TestOfflineSearchBootstrapFencesRosterAfterEachPage(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":1}`)
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceBootstrapStore{configs: []wkdb.ChannelClusterConfig{cfg}, physical: 2}
	rosterCalls := 0
	roster := func() ([]uint64, error) {
		rosterCalls++
		if rosterCalls >= 3 {
			return []uint64{1, 2}, nil
		}
		return []uint64{1}, nil
	}
	err := applySearchSourceOfflineBootstrapMarker(markerPath, 1, roster,
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil },
		store,
	)
	if !errors.Is(err, errSearchSourceRoster) {
		t.Fatalf("error = %v, want post-page roster fence", err)
	}
	if rosterCalls != 3 {
		t.Fatalf("roster calls = %d, want 3", rosterCalls)
	}
	if _, err := os.Stat(markerPath + ".applying"); err != nil {
		t.Fatalf("failed fenced bootstrap did not retain applying marker: %v", err)
	}
}

func TestOfflineSearchBootstrapFencesConfigRevisionBeforeConsumingMarker(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":1}`)
	cfg := validSearchSourceConfig()
	store := &fakeSearchSourceBootstrapStore{
		configs:         []wkdb.ChannelClusterConfig{cfg},
		physical:        2,
		configRevisions: []uint64{2, 2, 4},
	}
	err := applySearchSourceOfflineBootstrapMarker(
		markerPath,
		1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return cfg, nil },
		store,
	)
	if !errors.Is(err, errSearchSourceFence) {
		t.Fatalf("error = %v, want config revision fence", err)
	}
	if _, err := os.Stat(markerPath + ".applying"); err != nil {
		t.Fatalf("fenced bootstrap did not retain applying marker: %v", err)
	}
	if _, err := os.Stat(markerPath + ".consumed"); !os.IsNotExist(err) {
		t.Fatalf("fenced bootstrap consumed its marker: %v", err)
	}
}

func TestOfflineSearchBootstrapRejectsInvalidMarkerWithoutWriting(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
	writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":2}`)
	store := &fakeSearchSourceBootstrapStore{configs: []wkdb.ChannelClusterConfig{validSearchSourceConfig()}, physical: 2}
	err := applySearchSourceOfflineBootstrapMarker(
		markerPath, 1,
		func() ([]uint64, error) { return []uint64{1}, nil },
		func(string, uint8) (wkdb.ChannelClusterConfig, error) { return validSearchSourceConfig(), nil },
		store,
	)
	if err == nil {
		t.Fatal("wrong-node marker was accepted")
	}
	if store.updates != 0 {
		t.Fatalf("invalid marker updated %d watermarks", store.updates)
	}
}

func TestOfflineSearchBootstrapRejectsUnsafeMarkerFile(t *testing.T) {
	t.Run("permissions", func(t *testing.T) {
		markerPath := filepath.Join(t.TempDir(), SearchSourceOfflineBootstrapMarkerName)
		writeSearchSourceBootstrapMarker(t, markerPath, `{"version":1,"node_id":1}`)
		if err := os.Chmod(markerPath, 0644); err != nil {
			t.Fatal(err)
		}
		err := applySearchSourceOfflineBootstrapMarker(markerPath, 1,
			func() ([]uint64, error) { return []uint64{1}, nil },
			func(string, uint8) (wkdb.ChannelClusterConfig, error) { return validSearchSourceConfig(), nil },
			&fakeSearchSourceBootstrapStore{},
		)
		if err == nil {
			t.Fatal("world-readable marker was accepted")
		}
	})

	t.Run("symlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(dir, "target")
		writeSearchSourceBootstrapMarker(t, target, `{"version":1,"node_id":1}`)
		markerPath := filepath.Join(dir, SearchSourceOfflineBootstrapMarkerName)
		if err := os.Symlink(target, markerPath); err != nil {
			t.Fatal(err)
		}
		err := applySearchSourceOfflineBootstrapMarker(markerPath, 1,
			func() ([]uint64, error) { return []uint64{1}, nil },
			func(string, uint8) (wkdb.ChannelClusterConfig, error) { return validSearchSourceConfig(), nil },
			&fakeSearchSourceBootstrapStore{},
		)
		if err == nil {
			t.Fatal("symlink marker was silently ignored")
		}
	})
}

func writeSearchSourceBootstrapMarker(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
}

func testSearchSourceRPC(store *fakeSearchSourceStore) *rpc {
	return &rpc{
		searchSourceStore:  store,
		searchSourceReady:  func() error { return nil },
		searchSourceNodeID: func() uint64 { return 1 },
		searchSourceRoster: func() ([]uint64, error) { return []uint64{1}, nil },
		searchSourceAuthority: func(string, uint8) (wkdb.ChannelClusterConfig, error) {
			return validSearchSourceConfig(), nil
		},
	}
}

func validSearchSourceConfig() wkdb.ChannelClusterConfig {
	return wkdb.ChannelClusterConfig{
		Id: 9, ChannelId: "channel", ChannelType: 2, ReplicaMaxCount: 1,
		Replicas: []uint64{1}, LeaderId: 1, Term: 2, ConfVersion: 3,
		Status: wkdb.ChannelClusterStatusNormal,
	}
}

func validThreeNodeSearchSourceConfig(id uint64, channelID string, leaderID uint64) wkdb.ChannelClusterConfig {
	return wkdb.ChannelClusterConfig{
		Id: id, ChannelId: channelID, ChannelType: 2, ReplicaMaxCount: 3,
		Replicas: []uint64{1, 2, 3}, LeaderId: leaderID, Term: 2, ConfVersion: 3,
		Status: wkdb.ChannelClusterStatusNormal,
	}
}

type fakeSearchSourceStore struct {
	configs          []wkdb.ChannelClusterConfig
	configRevision   uint64
	configRevisions  []uint64
	applied          uint64
	physical         uint64
	messages         []wkdb.SearchSourceMessage
	calls            []string
	loadCalls        int
	revisionReadCall int
}

type fakeSearchSourceBootstrapStore struct {
	configs          []wkdb.ChannelClusterConfig
	configRevision   uint64
	configRevisions  []uint64
	applied          uint64
	physical         uint64
	reads            int
	updates          int
	limits           []int
	onGetConfigs     func()
	revisionReadCall int
}

func nextFakeSearchSourceConfigRevision(configured uint64, revisions []uint64, call *int) uint64 {
	if len(revisions) > 0 {
		index := *call
		if index >= len(revisions) {
			index = len(revisions) - 1
		}
		*call++
		return revisions[index]
	}
	if configured == 0 {
		return 2
	}
	return configured
}

func (f *fakeSearchSourceBootstrapStore) GetChannelClusterConfigRevision() uint64 {
	return nextFakeSearchSourceConfigRevision(f.configRevision, f.configRevisions, &f.revisionReadCall)
}

func (f *fakeSearchSourceBootstrapStore) GetChannelClusterConfigs(_ uint64, limit int) ([]wkdb.ChannelClusterConfig, error) {
	if f.onGetConfigs != nil {
		f.onGetConfigs()
		f.onGetConfigs = nil
	}
	f.reads++
	f.limits = append(f.limits, limit)
	return f.configs, nil
}

func (f *fakeSearchSourceBootstrapStore) GetAppliedMsgSeq(string, uint8) (uint64, error) {
	f.reads++
	return f.applied, nil
}

func (f *fakeSearchSourceBootstrapStore) GetLastMsgSeq(string, uint8) (uint64, error) {
	f.reads++
	return f.physical, nil
}

func (f *fakeSearchSourceBootstrapStore) UpdateAppliedMsgSeq(_ string, _ uint8, applied uint64) error {
	f.updates++
	f.applied = applied
	return nil
}

func (f *fakeSearchSourceStore) GetChannelClusterConfigs(uint64, int) ([]wkdb.ChannelClusterConfig, error) {
	f.calls = append(f.calls, "configs")
	return f.configs, nil
}

func (f *fakeSearchSourceStore) GetChannelClusterConfigRevision() uint64 {
	return nextFakeSearchSourceConfigRevision(f.configRevision, f.configRevisions, &f.revisionReadCall)
}

func (f *fakeSearchSourceStore) GetAppliedMsgSeq(string, uint8) (uint64, error) {
	f.calls = append(f.calls, "applied")
	return f.applied, nil
}

func (f *fakeSearchSourceStore) GetLastMsgSeq(string, uint8) (uint64, error) {
	f.calls = append(f.calls, "physical")
	return f.physical, nil
}

func (f *fakeSearchSourceStore) LoadNextRangeSearchSourceMessages(string, uint8, uint64, uint64, int, uint64) ([]wkdb.SearchSourceMessage, error) {
	f.calls = append(f.calls, "messages")
	f.loadCalls++
	return f.messages, nil
}
