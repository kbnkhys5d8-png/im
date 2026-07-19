package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestSearchSourceChannelsChecksSingleNodeRosterBeforeStore(t *testing.T) {
	store := &fakeSearchSourceStore{}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceRoster = func() ([]uint64, error) { return []uint64{1, 2}, nil }

	_, err := rpc.searchSourceChannels(searchSourceChannelPageRequest{Version: 5, Limit: 10})
	if !errors.Is(err, errSearchSourceRoster) {
		t.Fatalf("error = %v, want roster error", err)
	}
	if len(store.calls) != 0 {
		t.Fatalf("store was read before roster failed: %v", store.calls)
	}
}

func TestSearchSourceMessagesChecksSingleNodeRosterBeforeAuthorityOrStore(t *testing.T) {
	store := &fakeSearchSourceStore{}
	rpc := testSearchSourceRPC(store)
	rpc.searchSourceRoster = func() ([]uint64, error) { return []uint64{1, 2}, nil }
	authorityCalls := 0
	rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) {
		authorityCalls++
		return validSearchSourceConfig(), nil
	}
	_, err := rpc.searchSourceMessages(searchSourceMessageRequest{
		Version: 5, ChannelID: "channel", ChannelType: 2,
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3,
		NextSeq: 1, Limit: 10,
	})
	if !errors.Is(err, errSearchSourceRoster) {
		t.Fatalf("error = %v, want roster error", err)
	}
	if authorityCalls != 0 || len(store.calls) != 0 {
		t.Fatalf("authority/store read before roster failed: authority=%d store=%v", authorityCalls, store.calls)
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
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3,
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
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3,
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
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3,
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
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3,
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
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3,
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
		ExpectedLeaderID: 1, ExpectedTerm: 2, ExpectedConfigVersion: 3,
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

type fakeSearchSourceStore struct {
	configs   []wkdb.ChannelClusterConfig
	applied   uint64
	physical  uint64
	messages  []wkdb.SearchSourceMessage
	calls     []string
	loadCalls int
}

type fakeSearchSourceBootstrapStore struct {
	configs      []wkdb.ChannelClusterConfig
	applied      uint64
	physical     uint64
	reads        int
	updates      int
	limits       []int
	onGetConfigs func()
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
