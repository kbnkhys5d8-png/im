package plugin

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func getUnixSocket(socketPath string) (string, error) {
	socketDir := filepath.Dir(socketPath)
	if err := prepareSecureSocketDir(socketDir); err != nil {
		return "", fmt.Errorf("prepare plugin socket directory: %w", err)
	}

	info, err := os.Lstat(socketPath)
	if err == nil {
		if err := validateOwnedUnixSocket(info, false); err != nil {
			return "", fmt.Errorf("refuse to replace plugin socket path: %w", err)
		}
		if err := os.Remove(socketPath); err != nil {
			return "", fmt.Errorf("remove stale plugin socket: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("inspect plugin socket path: %w", err)
	}

	return fmt.Sprintf("unix://%s", socketPath), nil
}

func secureUnixSocket(socketPath string) error {
	if err := validateSecureSocketDir(filepath.Dir(socketPath)); err != nil {
		return err
	}
	return secureUnixSocketWithLstat(socketPath, os.Lstat)
}

func secureUnixSocketWithLstat(socketPath string, lstat func(string) (os.FileInfo, error)) error {
	before, err := lstat(socketPath)
	if err != nil {
		return err
	}
	if err := validateOwnedUnixSocket(before, false); err != nil {
		return err
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		return err
	}
	after, err := lstat(socketPath)
	if err != nil {
		return err
	}
	if !sameFileIdentity(before, after) {
		return errors.New("plugin socket path changed while securing it")
	}
	return validateOwnedUnixSocket(after, true)
}

func prepareSecureSocketDir(socketDir string) error {
	if err := os.MkdirAll(socketDir, 0700); err != nil {
		return err
	}
	before, err := os.Lstat(socketDir)
	if err != nil {
		return err
	}
	if err := validateOwnedSocketDir(before, false); err != nil {
		return err
	}

	dir, err := os.Open(socketDir)
	if err != nil {
		return err
	}
	defer dir.Close()
	opened, err := dir.Stat()
	if err != nil {
		return err
	}
	if !sameFileIdentity(before, opened) {
		return errors.New("plugin socket directory changed while opening it")
	}
	if err := validateOwnedSocketDir(opened, false); err != nil {
		return err
	}
	if err := dir.Chmod(0700); err != nil {
		return err
	}
	secured, err := dir.Stat()
	if err != nil {
		return err
	}
	if err := validateOwnedSocketDir(secured, true); err != nil {
		return err
	}
	after, err := os.Lstat(socketDir)
	if err != nil {
		return err
	}
	if !sameFileIdentity(secured, after) {
		return errors.New("plugin socket directory path changed while securing it")
	}
	return validateOwnedSocketDir(after, true)
}

func validateSecureSocketDir(socketDir string) error {
	info, err := os.Lstat(socketDir)
	if err != nil {
		return err
	}
	return validateOwnedSocketDir(info, true)
}

func validateOwnedSocketDir(info os.FileInfo, requireSecureMode bool) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("plugin socket directory must not be a symlink")
	}
	if !info.IsDir() {
		return errors.New("plugin socket parent is not a directory")
	}
	if err := validateCurrentOwner(info, "plugin socket directory"); err != nil {
		return err
	}
	if requireSecureMode && info.Mode().Perm() != 0700 {
		return fmt.Errorf("plugin socket directory mode is %o, want 700", info.Mode().Perm())
	}
	return nil
}

func validateOwnedUnixSocket(info os.FileInfo, requireSecureMode bool) error {
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("plugin socket must not be a symlink")
	}
	if info.Mode().Type() != os.ModeSocket {
		return errors.New("plugin socket path is not a unix socket")
	}
	if err := validateCurrentOwner(info, "plugin socket"); err != nil {
		return err
	}
	if requireSecureMode && info.Mode().Perm() != 0600 {
		return fmt.Errorf("plugin socket mode is %o, want 600", info.Mode().Perm())
	}
	return nil
}

func validateCurrentOwner(info os.FileInfo, label string) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("%s owner is unavailable", label)
	}
	if stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("%s owner uid is %d, want %d", label, stat.Uid, os.Geteuid())
	}
	return nil
}

func sameFileIdentity(left, right os.FileInfo) bool {
	leftStat, leftOK := left.Sys().(*syscall.Stat_t)
	rightStat, rightOK := right.Sys().(*syscall.Stat_t)
	return leftOK && rightOK && leftStat.Dev == rightStat.Dev && leftStat.Ino == rightStat.Ino
}

func waitForSecureUnixSocket(socketPath string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		err := secureUnixSocket(socketPath)
		if err == nil {
			return nil
		}
		if !os.IsNotExist(err) || time.Now().After(deadline) {
			return fmt.Errorf("secure plugin unix socket: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
