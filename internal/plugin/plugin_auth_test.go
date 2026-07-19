package plugin

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestLocalPluginAuthorizerRequiresManagedPIDUIDAndLivePIDFD(t *testing.T) {
	backend := &fakeLocalAuthBackend{
		credentials: map[int]localPeerCredentials{10: {PID: 123, UID: 501}},
		pidfds:      map[int]int{123: 77},
		alive:       map[int]bool{77: true},
	}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeStart(10, searchSourcePluginNo); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeRequest(10, searchSourcePluginNo); err != nil {
		t.Fatalf("authorized request rejected: %v", err)
	}

	backend.credentials[11] = localPeerCredentials{PID: 999, UID: 501}
	if err := auth.authorizeStart(11, searchSourcePluginNo); err == nil {
		t.Fatal("unmanaged pid was authorized")
	}
	backend.credentials[12] = localPeerCredentials{PID: 123, UID: 502}
	if err := auth.authorizeStart(12, searchSourcePluginNo); err == nil {
		t.Fatal("wrong uid was authorized")
	}
	backend.alive[77] = false
	if err := auth.authorizeRequest(10, searchSourcePluginNo); err == nil {
		t.Fatal("dead pidfd was authorized")
	}
}

func TestLocalPluginAuthorizerRevokesExitedProcess(t *testing.T) {
	backend := &fakeLocalAuthBackend{
		credentials: map[int]localPeerCredentials{10: {PID: 123, UID: 501}},
		pidfds:      map[int]int{123: 77},
		alive:       map[int]bool{77: true},
	}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeStart(10, searchSourcePluginNo); err != nil {
		t.Fatal(err)
	}
	auth.unmanageSearchProcess(123)
	if err := auth.authorizeRequest(10, searchSourcePluginNo); err == nil {
		t.Fatal("revoked process remained authorized")
	}
	if len(backend.closed) != 1 || backend.closed[0] != 77 {
		t.Fatalf("closed pidfds = %v, want [77]", backend.closed)
	}
}

func TestLocalPluginAuthorizerAllowsOnlyOneLiveManagedSearchProcess(t *testing.T) {
	backend := &fakeLocalAuthBackend{
		credentials: map[int]localPeerCredentials{
			10: {PID: 123, UID: 501},
			11: {PID: 124, UID: 501},
		},
		pidfds: map[int]int{123: 77, 124: 78},
		alive:  map[int]bool{77: true, 78: true},
	}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeStart(10, searchSourcePluginNo); err != nil {
		t.Fatal(err)
	}
	if err := auth.manageSearchProcess(124, searchSourcePluginName); err == nil {
		t.Fatal("second live search process was registered")
	}
	if err := auth.authorizeRequest(10, searchSourcePluginNo); err != nil {
		t.Fatalf("authoritative process was revoked by rejected replacement: %v", err)
	}

	backend.alive[77] = false
	auth.unmanageSearchProcess(123)
	if err := auth.manageSearchProcess(124, searchSourcePluginName); err != nil {
		t.Fatalf("exited process could not be replaced: %v", err)
	}
	if err := auth.authorizeRequest(10, searchSourcePluginNo); err == nil {
		t.Fatal("old process connection remained authorized after replacement")
	}
	if err := auth.authorizeStart(11, searchSourcePluginNo); err != nil {
		t.Fatalf("replacement process was not authorized: %v", err)
	}
}

func TestLocalPluginAuthorizerWaitsForParentProcessRegistration(t *testing.T) {
	backend := &fakeLocalAuthBackend{
		credentials: map[int]localPeerCredentials{10: {PID: 123, UID: 501}},
		pidfds:      map[int]int{123: 77},
		alive:       map[int]bool{77: true},
	}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	result := make(chan error, 1)
	go func() {
		result <- auth.authorizeStart(10, searchSourcePluginNo)
	}()

	deadline := time.Now().Add(time.Second)
	for backend.credentialCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if backend.credentialCalls.Load() == 0 {
		t.Fatal("authorization never inspected peer credentials")
	}
	select {
	case err := <-result:
		t.Fatalf("authorization returned before parent registration: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("authorization failed after parent registration: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("authorization did not resume after parent registration")
	}
	if backend.credentialCalls.Load() < 2 {
		t.Fatalf("peer credentials checked %d times, want at least 2", backend.credentialCalls.Load())
	}
}

func TestLocalPluginAuthorizerRevokesOnlyExactAuthorizedConnection(t *testing.T) {
	backend := &fakeLocalAuthBackend{
		credentials: map[int]localPeerCredentials{10: {PID: 123, UID: 501}},
		pidfds:      map[int]int{123: 77},
		alive:       map[int]bool{77: true},
	}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeStart(10, searchSourcePluginNo); err != nil {
		t.Fatal(err)
	}
	if auth.revokeConnection(11) {
		t.Fatal("unrelated fd revoked the authoritative connection")
	}
	if err := auth.authorizeRequest(10, searchSourcePluginNo); err != nil {
		t.Fatalf("authoritative connection was revoked: %v", err)
	}
	if !auth.revokeConnection(10) {
		t.Fatal("authorized fd was not revoked")
	}
	if err := auth.authorizeRequest(10, searchSourcePluginNo); err == nil {
		t.Fatal("revoked fd remained authorized")
	}
}

func TestLocalPluginAuthorizerStaleCloseCannotRevokeReusedFD(t *testing.T) {
	backend := &fakeLocalAuthBackend{
		credentials: map[int]localPeerCredentials{10: {PID: 123, UID: 501}},
		pidfds:      map[int]int{123: 77},
		alive:       map[int]bool{77: true},
	}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeStart(10, searchSourcePluginNo, "old-connection"); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeStart(10, searchSourcePluginNo, "replacement-connection"); err != nil {
		t.Fatal(err)
	}
	if auth.revokeConnection(10, "old-connection") {
		t.Fatal("stale close revoked a replacement that reused the same fd")
	}
	if err := auth.authorizeRequest(10, searchSourcePluginNo, "replacement-connection"); err != nil {
		t.Fatalf("replacement connection was revoked: %v", err)
	}
}

func TestLocalPluginAuthorizerWaitsForManagedProcessExit(t *testing.T) {
	backend := &fakeLocalAuthBackend{pidfds: map[int]int{123: 77}, alive: map[int]bool{77: true}}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- auth.waitForNoManagedSearchProcess(ctx) }()
	select {
	case err := <-result:
		t.Fatalf("wait returned before process exit: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	auth.unmanageSearchProcess(123)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("wait failed after process exit: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("wait did not resume after process exit")
	}
}

func TestLocalPluginAuthorizerProcessExitWaitTimesOutWithoutRevokingAuthority(t *testing.T) {
	backend := &fakeLocalAuthBackend{pidfds: map[int]int{123: 77}, alive: map[int]bool{77: true}}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := auth.waitForNoManagedSearchProcess(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait error = %v, want deadline exceeded", err)
	}
	if len(auth.managed) != 1 {
		t.Fatal("timeout revoked the still-running authoritative process")
	}
}

func TestLocalPluginAuthorizerFallsBackToStableProcIdentityWhenPIDFDIsUnavailable(t *testing.T) {
	backend := &fakeLocalAuthBackend{
		credentials:  map[int]localPeerCredentials{10: {PID: 123, UID: 501}},
		openErrors:   map[int]error{123: errPIDFDUnsupported},
		processAlive: map[int]bool{123: true},
		identities:   map[int]localProcessIdentity{123: {PID: 123, UID: 501, StartTime: 99}},
	}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeStart(10, searchSourcePluginNo); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeRequest(10, searchSourcePluginNo); err != nil {
		t.Fatalf("stable /proc identity was rejected: %v", err)
	}
	original := localProcessIdentity{PID: 123, UID: 501, StartTime: 99}
	for _, test := range []struct {
		name   string
		mutate func()
	}{
		{name: "dead process", mutate: func() { backend.processAlive[123] = false }},
		{name: "pid changed", mutate: func() { backend.identities[123] = localProcessIdentity{PID: 124, UID: 501, StartTime: 99} }},
		{name: "uid changed", mutate: func() { backend.identities[123] = localProcessIdentity{PID: 123, UID: 502, StartTime: 99} }},
		{name: "start time changed", mutate: func() { backend.identities[123] = localProcessIdentity{PID: 123, UID: 501, StartTime: 100} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend.processAlive[123] = true
			backend.identities[123] = original
			test.mutate()
			if err := auth.authorizeRequest(10, searchSourcePluginNo); err == nil {
				t.Fatal("changed /proc process identity was authorized")
			}
		})
	}
}

func TestLocalPluginAuthorizerFallsBackWhenPIDFDIsBlockedBySeccomp(t *testing.T) {
	backend := &fakeLocalAuthBackend{
		credentials:  map[int]localPeerCredentials{10: {PID: 123, UID: 501}},
		openErrors:   map[int]error{123: errors.Join(errPIDFDUnsupported, syscall.EPERM)},
		processAlive: map[int]bool{123: true},
		identities:   map[int]localProcessIdentity{123: {PID: 123, UID: 501, StartTime: 99}},
	}
	auth := newLocalPluginAuthorizerWithBackend(backend, 501)
	if err := auth.manageSearchProcess(123, searchSourcePluginName); err != nil {
		t.Fatal(err)
	}
	if err := auth.authorizeStart(10, searchSourcePluginNo); err != nil {
		t.Fatalf("stable /proc fallback was rejected after pidfd EPERM: %v", err)
	}
}

func TestSecurePluginSocketPermissions(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "wk-plugin-socket-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	dir := root + "/socket"
	path := dir + "/plugin.sock"
	if _, err := getUnixSocket(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("socket directory mode = %o, want 700", info.Mode().Perm())
	}
	if err := os.WriteFile(path, []byte("fixture"), 0666); err != nil {
		t.Fatal(err)
	}
	if err := secureUnixSocketWithLstat(path, func(string) (os.FileInfo, error) {
		fixtureInfo, statErr := os.Stat(path)
		if statErr != nil {
			return nil, statErr
		}
		return socketModeFileInfo{FileInfo: fixtureInfo}, nil
	}); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %o, want 600", info.Mode().Perm())
	}
}

type socketModeFileInfo struct{ os.FileInfo }

func (s socketModeFileInfo) Mode() os.FileMode {
	return os.ModeSocket | s.FileInfo.Mode().Perm()
}

type socketModeOwnerFileInfo struct {
	os.FileInfo
	uid uint32
}

func (s socketModeOwnerFileInfo) Mode() os.FileMode {
	return os.ModeSocket | s.FileInfo.Mode().Perm()
}

func (s socketModeOwnerFileInfo) Sys() any {
	stat := *s.FileInfo.Sys().(*syscall.Stat_t)
	stat.Uid = s.uid
	return &stat
}

func TestSecurePluginSocketRejectsRegularFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "not-a-socket")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := secureUnixSocket(path); err == nil {
		t.Fatal("regular file was accepted as plugin socket")
	}
}

func TestGetUnixSocketRejectsSymlinkParent(t *testing.T) {
	root := t.TempDir()
	realDir := filepath.Join(root, "real")
	if err := os.Mkdir(realDir, 0700); err != nil {
		t.Fatal(err)
	}
	symlinkDir := filepath.Join(root, "socket")
	if err := os.Symlink(realDir, symlinkDir); err != nil {
		t.Fatal(err)
	}
	if _, err := getUnixSocket(filepath.Join(symlinkDir, "plugin.sock")); err == nil {
		t.Fatal("symlink socket parent was accepted")
	}
}

func TestSecurePluginSocketRejectsWrongOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plugin.sock")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	fixtureInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	wrongUID := uint32(os.Geteuid()) + 1
	if err := secureUnixSocketWithLstat(path, func(string) (os.FileInfo, error) {
		return socketModeOwnerFileInfo{FileInfo: fixtureInfo, uid: wrongUID}, nil
	}); err == nil {
		t.Fatal("socket owned by another uid was accepted")
	}
}

func TestSecurePluginSocketRejectsPathReplacement(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "plugin.sock")
	replacement := filepath.Join(root, "replacement.sock")
	if err := os.WriteFile(path, []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(replacement, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(replacement)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	if err := secureUnixSocketWithLstat(path, func(string) (os.FileInfo, error) {
		calls++
		if calls == 1 {
			return socketModeFileInfo{FileInfo: before}, nil
		}
		return socketModeFileInfo{FileInfo: after}, nil
	}); err == nil {
		t.Fatal("socket path replacement was accepted")
	}
}

type fakeLocalAuthBackend struct {
	credentials     map[int]localPeerCredentials
	pidfds          map[int]int
	openErrors      map[int]error
	alive           map[int]bool
	processAlive    map[int]bool
	identities      map[int]localProcessIdentity
	closed          []int
	credentialCalls atomic.Int32
}

func (f *fakeLocalAuthBackend) peerCredentials(fd int) (localPeerCredentials, error) {
	f.credentialCalls.Add(1)
	credentials, ok := f.credentials[fd]
	if !ok {
		return localPeerCredentials{}, errors.New("peer unavailable")
	}
	return credentials, nil
}

func (f *fakeLocalAuthBackend) openPID(pid int) (int, error) {
	if err := f.openErrors[pid]; err != nil {
		return -1, err
	}
	pidfd, ok := f.pidfds[pid]
	if !ok {
		return -1, errors.New("pid unavailable")
	}
	return pidfd, nil
}

func (f *fakeLocalAuthBackend) pidProcessAlive(pid int) error {
	if !f.processAlive[pid] {
		return errors.New("process exited")
	}
	return nil
}

func (f *fakeLocalAuthBackend) processIdentity(pid int) (localProcessIdentity, error) {
	identity, ok := f.identities[pid]
	if !ok {
		return localProcessIdentity{}, errors.New("process identity unavailable")
	}
	return identity, nil
}

func (f *fakeLocalAuthBackend) pidAlive(pidfd int) error {
	if !f.alive[pidfd] {
		return errors.New("process exited")
	}
	return nil
}

func (f *fakeLocalAuthBackend) closePID(pidfd int) error {
	f.closed = append(f.closed, pidfd)
	return nil
}
