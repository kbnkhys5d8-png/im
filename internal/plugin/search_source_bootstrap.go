package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

const (
	SearchSourceOfflineBootstrapMarkerName = "search-source-bootstrap-v1.json"
	searchSourceBootstrapPageSize          = 100
	searchSourceBootstrapMarkerMaxBytes    = 4096
	searchSourceAuthorityRetryInterval     = 50 * time.Millisecond
	searchSourceRecoveryAuthorizedSuffix   = ".recovery-authorized"
	searchSourceRecoveryApplyingSuffix     = ".recovery-applying"
	searchSourceRecoveryConsumedSuffix     = ".recovery-consumed"
)

var errSearchSourceBootstrapRequired = errors.New("explicit offline search bootstrap is required")

type searchSourceOfflineBootstrapMarker struct {
	Version int    `json:"version"`
	NodeID  uint64 `json:"node_id"`
}

type searchSourceBootstrapStore interface {
	GetChannelClusterConfigs(afterID uint64, limit int) ([]wkdb.ChannelClusterConfig, error)
	GetChannelClusterConfigRevision() uint64
	GetAppliedMsgSeq(channelID string, channelType uint8) (uint64, error)
	GetLastMsgSeq(channelID string, channelType uint8) (uint64, error)
	UpdateAppliedMsgSeq(channelID string, channelType uint8, applied uint64) error
}

type liveSearchSourceBootstrapStore struct{ liveSearchSourceStore }

func (liveSearchSourceBootstrapStore) UpdateAppliedMsgSeq(channelID string, channelType uint8, applied uint64) error {
	if service.Store == nil {
		return errors.New("store is unavailable")
	}
	return service.Store.DB().UpdateChannelAppliedIndex(channelID, channelType, applied)
}

// ApplySearchSourceOfflineBootstrapMarker is called only in the startup gap
// after cluster metadata is authoritative and before client/API/plugin traffic
// starts. Absence of an explicitly supplied marker closes the one-time window
// and keeps search fail-closed, but never prevents ordinary IM startup.
func ApplySearchSourceOfflineBootstrapMarker(ctx context.Context, markerPath string) (bool, error) {
	err := applySearchSourceOfflineBootstrapMarkerUntilStable(
		ctx,
		markerPath,
		defaultSearchSourceNodeID(),
		defaultSearchSourceRoster,
		defaultSearchSourceAuthority,
		liveSearchSourceBootstrapStore{},
	)
	if err != nil {
		return false, err
	}
	var consumedPath string
	for _, candidate := range []string{markerPath + ".consumed", markerPath + searchSourceRecoveryConsumedSuffix} {
		consumed, err := searchSourceBootstrapPathExists(candidate)
		if err != nil {
			return false, err
		}
		if consumed {
			if consumedPath != "" {
				return false, errors.New("offline search bootstrap has multiple consumed markers")
			}
			consumedPath = candidate
		}
	}
	if consumedPath == "" {
		return false, errSearchSourceBootstrapRequired
	}
	if _, err := readSearchSourceBootstrapMarker(consumedPath, defaultSearchSourceNodeID()); err != nil {
		return false, err
	}
	return true, nil
}

func applySearchSourceOfflineBootstrapMarker(
	markerPath string,
	nodeID uint64,
	roster func() ([]uint64, error),
	authority func(string, uint8) (wkdb.ChannelClusterConfig, error),
	store searchSourceBootstrapStore,
) error {
	return applySearchSourceOfflineBootstrapMarkerContext(context.Background(), markerPath, nodeID, roster, authority, store)
}

func applySearchSourceOfflineBootstrapMarkerUntilStable(
	ctx context.Context,
	markerPath string,
	nodeID uint64,
	roster func() ([]uint64, error),
	authority func(string, uint8) (wkdb.ChannelClusterConfig, error),
	store searchSourceBootstrapStore,
) error {
	for {
		err := applySearchSourceOfflineBootstrapMarkerContext(ctx, markerPath, nodeID, roster, authority, store)
		if !errors.Is(err, errSearchSourceFence) {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf(
				"wait for stable search source configuration: %w",
				errors.Join(err, ctx.Err()),
			)
		case <-time.After(searchSourceAuthorityRetryInterval):
		}
	}
}

func applySearchSourceOfflineBootstrapMarkerContext(
	ctx context.Context,
	markerPath string,
	nodeID uint64,
	roster func() ([]uint64, error),
	authority func(string, uint8) (wkdb.ChannelClusterConfig, error),
	store searchSourceBootstrapStore,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	applyingPath := markerPath + ".applying"
	consumedPath := markerPath + ".consumed"
	closedPath := markerPath + ".window-closed"
	recoveryPath := markerPath + searchSourceRecoveryAuthorizedSuffix
	recoveryApplyingPath := markerPath + searchSourceRecoveryApplyingSuffix
	recoveryConsumedPath := markerPath + searchSourceRecoveryConsumedSuffix
	markerExists, err := searchSourceBootstrapPathExists(markerPath)
	if err != nil {
		return fmt.Errorf("inspect offline search bootstrap marker: %w", err)
	}
	applyingExists, err := searchSourceBootstrapPathExists(applyingPath)
	if err != nil {
		return fmt.Errorf("inspect applying offline search bootstrap marker: %w", err)
	}
	if nodeID == 0 || roster == nil || authority == nil || store == nil {
		return errors.New("offline search bootstrap dependencies are unavailable")
	}
	consumedExists, err := searchSourceBootstrapPathExists(consumedPath)
	if err != nil {
		return fmt.Errorf("inspect consumed offline search bootstrap marker: %w", err)
	}
	closedExists, err := searchSourceBootstrapPathExists(closedPath)
	if err != nil {
		return fmt.Errorf("inspect closed offline search bootstrap window: %w", err)
	}
	recoveryExists, err := searchSourceBootstrapPathExists(recoveryPath)
	if err != nil {
		return fmt.Errorf("inspect offline search recovery authorization: %w", err)
	}
	recoveryApplyingExists, err := searchSourceBootstrapPathExists(recoveryApplyingPath)
	if err != nil {
		return fmt.Errorf("inspect applying offline search recovery: %w", err)
	}
	recoveryConsumedExists, err := searchSourceBootstrapPathExists(recoveryConsumedPath)
	if err != nil {
		return fmt.Errorf("inspect consumed offline search recovery: %w", err)
	}
	recoveryStateExists := recoveryExists || recoveryApplyingExists || recoveryConsumedExists
	if recoveryStateExists && !closedExists {
		return errors.New("offline search recovery requires an existing closed-window marker")
	}
	if closedExists {
		if markerExists || applyingExists || consumedExists {
			return errors.New("offline search bootstrap has conflicting normal and closed-window state")
		}
		return applyClosedSearchSourceRecovery(
			ctx,
			markerPath,
			nodeID,
			roster,
			authority,
			store,
			closedPath,
			recoveryPath,
			recoveryApplyingPath,
			recoveryConsumedPath,
			recoveryExists,
			recoveryApplyingExists,
			recoveryConsumedExists,
		)
	}
	if !markerExists && !applyingExists {
		if consumedExists {
			if _, err := readSearchSourceBootstrapMarker(consumedPath, nodeID); err != nil {
				return fmt.Errorf("validate consumed offline search bootstrap marker: %w", err)
			}
			return reconcileConsumedSearchSource(ctx, nodeID, roster, authority, store)
		}
		if err := closeSearchSourceBootstrapWindow(closedPath, markerPath, nodeID); err != nil {
			return err
		}
		return errSearchSourceBootstrapRequired
	}
	if consumedExists || closedExists {
		return errors.New("offline search bootstrap marker was already consumed")
	}
	if markerExists && applyingExists {
		return errors.New("offline search bootstrap has both pending and applying markers")
	}
	activePath := applyingPath
	if markerExists {
		activePath = markerPath
	}
	marker, err := readSearchSourceBootstrapMarker(activePath, nodeID)
	if err != nil {
		return err
	}
	if marker.Version != 1 || marker.NodeID != nodeID {
		return errors.New("offline search bootstrap marker does not match protocol or local node")
	}
	if markerExists {
		if err := os.Rename(markerPath, applyingPath); err != nil {
			return fmt.Errorf("claim offline search bootstrap marker: %w", err)
		}
		if err := syncSearchSourceBootstrapDir(markerPath); err != nil {
			return err
		}
	}

	if err := runSearchSourceOfflineBootstrap(ctx, nodeID, roster, authority, store); err != nil {
		return err
	}
	consumedExists, err = searchSourceBootstrapPathExists(consumedPath)
	if err != nil {
		return fmt.Errorf("inspect consumed offline search bootstrap marker: %w", err)
	}
	if consumedExists {
		return errors.New("offline search bootstrap consumed marker already exists")
	}
	if err := os.Rename(applyingPath, consumedPath); err != nil {
		return fmt.Errorf("consume offline search bootstrap marker: %w", err)
	}
	return syncSearchSourceBootstrapDir(markerPath)
}

func applyClosedSearchSourceRecovery(
	ctx context.Context,
	markerPath string,
	nodeID uint64,
	roster func() ([]uint64, error),
	authority func(string, uint8) (wkdb.ChannelClusterConfig, error),
	store searchSourceBootstrapStore,
	closedPath string,
	recoveryPath string,
	recoveryApplyingPath string,
	recoveryConsumedPath string,
	recoveryExists bool,
	recoveryApplyingExists bool,
	recoveryConsumedExists bool,
) error {
	if _, err := readSearchSourceBootstrapMarker(closedPath, nodeID); err != nil {
		return fmt.Errorf("validate closed offline search bootstrap window: %w", err)
	}
	activeStates := 0
	for _, exists := range []bool{recoveryExists, recoveryApplyingExists, recoveryConsumedExists} {
		if exists {
			activeStates++
		}
	}
	if activeStates > 1 {
		return errors.New("offline search recovery has conflicting state markers")
	}
	if recoveryConsumedExists {
		if _, err := readSearchSourceBootstrapMarker(recoveryConsumedPath, nodeID); err != nil {
			return fmt.Errorf("validate consumed offline search recovery marker: %w", err)
		}
		return reconcileConsumedSearchSource(ctx, nodeID, roster, authority, store)
	}
	if !recoveryExists && !recoveryApplyingExists {
		return errSearchSourceBootstrapRequired
	}
	activePath := recoveryApplyingPath
	if recoveryExists {
		activePath = recoveryPath
	}
	marker, err := readSearchSourceBootstrapMarker(activePath, nodeID)
	if err != nil {
		return err
	}
	if marker.Version != 1 || marker.NodeID != nodeID {
		return errors.New("offline search recovery marker does not match protocol or local node")
	}
	if recoveryExists {
		if err := os.Rename(recoveryPath, recoveryApplyingPath); err != nil {
			return fmt.Errorf("claim offline search recovery marker: %w", err)
		}
		if err := syncSearchSourceBootstrapDir(markerPath); err != nil {
			return err
		}
	}
	if err := runSearchSourceOfflineBootstrap(ctx, nodeID, roster, authority, store); err != nil {
		return err
	}
	consumedExists, err := searchSourceBootstrapPathExists(recoveryConsumedPath)
	if err != nil {
		return fmt.Errorf("inspect consumed offline search recovery marker: %w", err)
	}
	if consumedExists {
		return errors.New("offline search recovery consumed marker already exists")
	}
	if err := os.Rename(recoveryApplyingPath, recoveryConsumedPath); err != nil {
		return fmt.Errorf("consume offline search recovery marker: %w", err)
	}
	return syncSearchSourceBootstrapDir(markerPath)
}

// reconcileConsumedSearchSource closes the async-observer crash window only
// after a previous startup durably consumed the explicit v5 bootstrap marker.
// A window-closed installation never reaches this path, so an old database is
// not silently initialized without operator authorization.
func reconcileConsumedSearchSource(
	ctx context.Context,
	nodeID uint64,
	roster func() ([]uint64, error),
	authority func(string, uint8) (wkdb.ChannelClusterConfig, error),
	store searchSourceBootstrapStore,
) error {
	expectedRoster, err := loadSearchSourceBootstrapRoster(nodeID, roster)
	if err != nil {
		return err
	}
	expectedConfigRevision, err := stableSearchSourceConfigRevision(store.GetChannelClusterConfigRevision())
	if err != nil {
		return err
	}
	validateSnapshot := func() error {
		current, err := loadSearchSourceBootstrapRoster(nodeID, roster)
		if err != nil {
			return err
		}
		if !equalSearchSourceRoster(expectedRoster, current) {
			return errSearchSourceRoster
		}
		currentRevision, err := stableSearchSourceConfigRevision(store.GetChannelClusterConfigRevision())
		if err != nil || currentRevision != expectedConfigRevision {
			return errSearchSourceFence
		}
		return nil
	}
	var afterID uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateSnapshot(); err != nil {
			return err
		}
		configs, err := store.GetChannelClusterConfigs(afterID, searchSourceBootstrapPageSize)
		if err != nil {
			return fmt.Errorf("scan consumed search bootstrap channels: %w", err)
		}
		if len(configs) == 0 {
			break
		}
		for _, before := range configs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if before.Id <= afterID || !validSearchSourceClusterConfig(before, expectedRoster) {
				return errSearchSourceFence
			}
			afterID = before.Id
			if !searchSourceConfigHasReplica(before, nodeID) {
				continue
			}
			authoritativeBefore, err := loadSearchSourceAuthority(
				ctx,
				authority,
				before.ChannelId,
				before.ChannelType,
			)
			if err != nil {
				return err
			}
			if !before.Equal(authoritativeBefore) || !validSearchSourceClusterConfig(authoritativeBefore, expectedRoster) ||
				!searchSourceConfigHasReplica(authoritativeBefore, nodeID) {
				return errSearchSourceFence
			}
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				applied, err := store.GetAppliedMsgSeq(before.ChannelId, before.ChannelType)
				if err != nil {
					return err
				}
				physical, err := store.GetLastMsgSeq(before.ChannelId, before.ChannelType)
				if err != nil {
					return err
				}
				if applied > physical || physical > wkdb.MaxMessageSequence {
					return fmt.Errorf("invalid consumed search watermarks: applied=%d physical=%d", applied, physical)
				}
				if applied == physical {
					break
				}
				if err := store.UpdateAppliedMsgSeq(before.ChannelId, before.ChannelType, physical); err != nil {
					return fmt.Errorf("reconcile consumed search watermark: %w", err)
				}
				confirmed, err := store.GetAppliedMsgSeq(before.ChannelId, before.ChannelType)
				if err != nil {
					return err
				}
				if confirmed < physical {
					return fmt.Errorf("consumed search watermark was not durable: got=%d want-at-least=%d", confirmed, physical)
				}
			}
			authoritativeAfter, err := loadSearchSourceAuthority(
				ctx,
				authority,
				before.ChannelId,
				before.ChannelType,
			)
			if err != nil {
				return err
			}
			if !authoritativeBefore.Equal(authoritativeAfter) {
				return errSearchSourceFence
			}
			if err := validateSnapshot(); err != nil {
				return err
			}
		}
		if len(configs) < searchSourceBootstrapPageSize {
			break
		}
	}
	return validateSnapshot()
}

func runSearchSourceOfflineBootstrap(
	ctx context.Context,
	nodeID uint64,
	roster func() ([]uint64, error),
	authority func(string, uint8) (wkdb.ChannelClusterConfig, error),
	store searchSourceBootstrapStore,
) error {
	expectedRoster, err := loadSearchSourceBootstrapRoster(nodeID, roster)
	if err != nil {
		return err
	}
	expectedConfigRevision, err := stableSearchSourceConfigRevision(store.GetChannelClusterConfigRevision())
	if err != nil {
		return err
	}
	validateSnapshot := func() error {
		current, err := loadSearchSourceBootstrapRoster(nodeID, roster)
		if err != nil {
			return err
		}
		if !equalSearchSourceRoster(expectedRoster, current) {
			return errSearchSourceRoster
		}
		currentRevision, err := stableSearchSourceConfigRevision(store.GetChannelClusterConfigRevision())
		if err != nil || currentRevision != expectedConfigRevision {
			return errSearchSourceFence
		}
		return nil
	}
	var afterID uint64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := validateSnapshot(); err != nil {
			return err
		}
		configs, err := store.GetChannelClusterConfigs(afterID, searchSourceBootstrapPageSize)
		if err != nil {
			return fmt.Errorf("scan offline search bootstrap channels: %w", err)
		}
		if len(configs) == 0 {
			break
		}
		for _, before := range configs {
			if err := ctx.Err(); err != nil {
				return err
			}
			if before.Id <= afterID || !validSearchSourceClusterConfig(before, expectedRoster) {
				return errSearchSourceFence
			}
			afterID = before.Id
			if !searchSourceConfigHasReplica(before, nodeID) {
				continue
			}
			applied, err := store.GetAppliedMsgSeq(before.ChannelId, before.ChannelType)
			if err != nil {
				return err
			}
			physical, err := store.GetLastMsgSeq(before.ChannelId, before.ChannelType)
			if err != nil {
				return err
			}
			if applied > physical || physical > wkdb.MaxMessageSequence {
				return fmt.Errorf("invalid offline bootstrap watermarks: applied=%d physical=%d", applied, physical)
			}
			authoritativeBefore, err := loadSearchSourceAuthority(
				ctx,
				authority,
				before.ChannelId,
				before.ChannelType,
			)
			if err != nil {
				return err
			}
			if !validSearchSourceClusterConfig(authoritativeBefore, expectedRoster) ||
				!searchSourceConfigHasReplica(authoritativeBefore, nodeID) || !before.Equal(authoritativeBefore) {
				return errSearchSourceFence
			}
			if applied > 0 && applied < physical {
				// A pre-existing partial watermark is not distinguishable from
				// legacy or operator-managed state. Do not consume the first-start
				// marker, otherwise a later consumed-marker recovery could silently
				// reclassify and advance it as an observer crash tail.
				return fmt.Errorf("offline search bootstrap found partial watermark: applied=%d physical=%d", applied, physical)
			}
			if applied == 0 && physical > 0 {
				if err := store.UpdateAppliedMsgSeq(before.ChannelId, before.ChannelType, physical); err != nil {
					return fmt.Errorf("initialize offline search applied watermark: %w", err)
				}
				confirmed, err := store.GetAppliedMsgSeq(before.ChannelId, before.ChannelType)
				if err != nil {
					return err
				}
				if confirmed != physical {
					return fmt.Errorf("offline search applied watermark was not durable: got=%d want=%d", confirmed, physical)
				}
			}
			authoritativeAfter, err := loadSearchSourceAuthority(
				ctx,
				authority,
				before.ChannelId,
				before.ChannelType,
			)
			if err != nil {
				return err
			}
			if !authoritativeBefore.Equal(authoritativeAfter) {
				return errSearchSourceFence
			}
			if err := validateSnapshot(); err != nil {
				return err
			}
		}
		if len(configs) < searchSourceBootstrapPageSize {
			break
		}
	}
	return validateSnapshot()
}

func loadSearchSourceAuthority(
	ctx context.Context,
	authority func(string, uint8) (wkdb.ChannelClusterConfig, error),
	channelID string,
	channelType uint8,
) (wkdb.ChannelClusterConfig, error) {
	for {
		config, err := authority(channelID, channelType)
		if err == nil {
			return config, nil
		}
		if !isSearchSourceAuthorityStartupError(err) {
			return wkdb.EmptyChannelClusterConfig, err
		}
		select {
		case <-ctx.Done():
			return wkdb.EmptyChannelClusterConfig, fmt.Errorf(
				"wait for authoritative search source config: %w",
				errors.Join(err, ctx.Err()),
			)
		case <-time.After(searchSourceAuthorityRetryInterval):
		}
	}
}

func isSearchSourceAuthorityStartupError(err error) bool {
	for current := err; current != nil; current = errors.Unwrap(current) {
		switch current.Error() {
		case "connect not authed", "conn is nil":
			return true
		}
	}
	return false
}

func loadSearchSourceBootstrapRoster(nodeID uint64, roster func() ([]uint64, error)) ([]uint64, error) {
	ids, err := roster()
	if err != nil {
		return nil, errors.Join(errSearchSourceRoster, err)
	}
	return canonicalSearchSourceRoster(nodeID, ids)
}

func searchSourceConfigHasReplica(cfg wkdb.ChannelClusterConfig, nodeID uint64) bool {
	for _, replicaID := range cfg.Replicas {
		if replicaID == nodeID {
			return true
		}
	}
	return false
}

func readSearchSourceBootstrapMarker(markerPath string, nodeID uint64) (searchSourceOfflineBootstrapMarker, error) {
	info, err := os.Lstat(markerPath)
	if err != nil {
		return searchSourceOfflineBootstrapMarker{}, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() <= 0 || info.Size() > searchSourceBootstrapMarkerMaxBytes {
		return searchSourceOfflineBootstrapMarker{}, errors.New("offline search bootstrap marker must be a non-empty 0600 regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return searchSourceOfflineBootstrapMarker{}, errors.New("offline search bootstrap marker must be owned by the IM user")
	}
	data, err := os.ReadFile(markerPath)
	if err != nil {
		return searchSourceOfflineBootstrapMarker{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var marker searchSourceOfflineBootstrapMarker
	if err := decoder.Decode(&marker); err != nil {
		return searchSourceOfflineBootstrapMarker{}, fmt.Errorf("decode offline search bootstrap marker: %w", err)
	}
	if err := ensureSearchSourceMarkerEOF(decoder); err != nil {
		return searchSourceOfflineBootstrapMarker{}, err
	}
	if marker.Version != 1 || marker.NodeID != nodeID {
		return searchSourceOfflineBootstrapMarker{}, errors.New("offline search bootstrap marker does not match protocol or local node")
	}
	return marker, nil
}

func ensureSearchSourceMarkerEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("offline search bootstrap marker contains trailing JSON")
		}
		return err
	}
	return nil
}

func searchSourceBootstrapPathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func syncSearchSourceBootstrapDir(markerPath string) error {
	dir, err := os.Open(filepath.Dir(markerPath))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func closeSearchSourceBootstrapWindow(closedPath, markerPath string, nodeID uint64) error {
	file, err := os.OpenFile(closedPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("close offline search bootstrap window: %w", err)
	}
	encodeErr := json.NewEncoder(file).Encode(searchSourceOfflineBootstrapMarker{Version: 1, NodeID: nodeID})
	if encodeErr == nil {
		encodeErr = file.Sync()
	}
	closeErr := file.Close()
	if encodeErr != nil {
		return fmt.Errorf("persist closed offline search bootstrap window: %w", encodeErr)
	}
	if closeErr != nil {
		return closeErr
	}
	return syncSearchSourceBootstrapDir(markerPath)
}
