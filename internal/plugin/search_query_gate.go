package plugin

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

const (
	searchQueryGateEnv        = "SEARCH_LOCAL_FAIL_CLOSED_FILE"
	searchQueryGatePath       = "/run/tsdd-search-control/usersearch-disabled-v1"
	searchQueryPluginPath     = "/usersearch"
	searchQueryUnavailableMsg = "search is locally disabled"
)

var errSearchQueryUnavailable = errors.New(searchQueryUnavailableMsg)

type searchQueryGateFunc func(pluginNo, routePath string) error

func defaultSearchQueryGate(pluginNo, routePath string) error {
	if pluginNo != searchSourcePluginNo || routePath != searchQueryPluginPath {
		return nil
	}
	value, configured := os.LookupEnv(searchQueryGateEnv)
	return inspectSearchQueryGate(value, configured, os.Lstat)
}

func inspectSearchQueryGate(value string, configured bool, lstat func(string) (os.FileInfo, error)) error {
	if !configured || value != searchQueryGatePath {
		return errSearchQueryUnavailable
	}
	parent := filepath.Dir(searchQueryGatePath)
	parentInfo, err := lstat(parent)
	if err != nil || parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() || !rootOwnedSafeMode(parentInfo) || parentInfo.Mode().Perm() != 0o700 {
		return errSearchQueryUnavailable
	}
	markerInfo, err := lstat(searchQueryGatePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil || markerInfo.Mode()&os.ModeSymlink != 0 || !markerInfo.Mode().IsRegular() || !rootOwnedSafeMode(markerInfo) {
		return errSearchQueryUnavailable
	}
	// A safe marker is the operator-controlled kill switch.
	return errSearchQueryUnavailable
}

func rootOwnedSafeMode(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == 0 && info.Mode().Perm()&0o022 == 0
}
