package plugin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	searchSourcePluginNo   = "wk.plugin.search"
	searchSourcePluginName = "wk.plugin.search.wkp"
)

var errSearchSourceUnauthorized = errors.New("search source plugin is not an authorized managed local process")
var errPIDFDUnsupported = errors.New("pidfd is unavailable on this kernel")

const searchSourceRegistrationWait = 100 * time.Millisecond

type localPeerCredentials struct {
	PID int
	UID uint32
}

type localProcessIdentity struct {
	PID       int
	UID       uint32
	StartTime uint64
}

type localAuthBackend interface {
	peerCredentials(fd int) (localPeerCredentials, error)
	openPID(pid int) (int, error)
	pidAlive(pidfd int) error
	pidProcessAlive(pid int) error
	processIdentity(pid int) (localProcessIdentity, error)
	closePID(pidfd int) error
}

type managedSearchProcess struct {
	pid   int
	uid   uint32
	pidfd int
	name  string
	proc  localProcessIdentity
}

type authorizedSearchConnection struct {
	pid      int
	identity any
}

type localPluginAuthorizer struct {
	mu               sync.Mutex
	backend          localAuthBackend
	euid             uint32
	managed          map[int]*managedSearchProcess
	authorized       map[int]authorizedSearchConnection
	managedChanged   chan struct{}
	registrationWait time.Duration
}

func newLocalPluginAuthorizer() *localPluginAuthorizer {
	return newLocalPluginAuthorizerWithBackend(newLocalAuthBackend(), uint32(os.Geteuid()))
}

func newLocalPluginAuthorizerWithBackend(backend localAuthBackend, euid uint32) *localPluginAuthorizer {
	return &localPluginAuthorizer{
		backend: backend, euid: euid,
		managed: make(map[int]*managedSearchProcess), authorized: make(map[int]authorizedSearchConnection),
		managedChanged: make(chan struct{}), registrationWait: searchSourceRegistrationWait,
	}
}

func (a *localPluginAuthorizer) manageSearchProcess(pid int, name string) error {
	if pid <= 0 || name != searchSourcePluginName {
		return errSearchSourceUnauthorized
	}
	record, err := a.openManagedSearchProcess(pid, name)
	if err != nil {
		return err
	}
	a.mu.Lock()
	if len(a.managed) != 0 {
		a.mu.Unlock()
		a.closeManagedSearchProcess(record)
		return errors.New("a managed search plugin process is already authoritative")
	}
	a.managed[pid] = record
	a.signalManagedChangedLocked()
	a.mu.Unlock()
	return nil
}

func (a *localPluginAuthorizer) openManagedSearchProcess(pid int, name string) (*managedSearchProcess, error) {
	pidfd, err := a.backend.openPID(pid)
	if err == nil {
		if aliveErr := a.backend.pidAlive(pidfd); aliveErr == nil {
			return &managedSearchProcess{pid: pid, uid: a.euid, pidfd: pidfd, name: name}, nil
		} else {
			_ = a.backend.closePID(pidfd)
			if !errors.Is(aliveErr, errPIDFDUnsupported) {
				return nil, fmt.Errorf("validate managed search pidfd: %w", aliveErr)
			}
		}
	} else if !errors.Is(err, errPIDFDUnsupported) {
		return nil, fmt.Errorf("open managed search pidfd: %w", err)
	}
	identity, err := a.stableProcessIdentity(pid)
	if err != nil {
		return nil, fmt.Errorf("validate managed search /proc identity: %w", err)
	}
	return &managedSearchProcess{pid: pid, uid: a.euid, pidfd: -1, name: name, proc: identity}, nil
}

func (a *localPluginAuthorizer) stableProcessIdentity(pid int) (localProcessIdentity, error) {
	if err := a.backend.pidProcessAlive(pid); err != nil {
		return localProcessIdentity{}, err
	}
	first, err := a.backend.processIdentity(pid)
	if err != nil {
		return localProcessIdentity{}, err
	}
	if first.PID != pid || first.UID != a.euid || first.StartTime == 0 {
		return localProcessIdentity{}, errSearchSourceUnauthorized
	}
	if err := a.backend.pidProcessAlive(pid); err != nil {
		return localProcessIdentity{}, err
	}
	second, err := a.backend.processIdentity(pid)
	if err != nil {
		return localProcessIdentity{}, err
	}
	if second != first {
		return localProcessIdentity{}, errors.New("managed search /proc identity changed during validation")
	}
	return first, nil
}

func (a *localPluginAuthorizer) validateManagedSearchProcess(record *managedSearchProcess) error {
	if record.pidfd >= 0 {
		return a.backend.pidAlive(record.pidfd)
	}
	identity, err := a.stableProcessIdentity(record.pid)
	if err != nil {
		return err
	}
	if identity != record.proc {
		return errors.New("managed search /proc identity changed")
	}
	return nil
}

func (a *localPluginAuthorizer) closeManagedSearchProcess(record *managedSearchProcess) {
	if record != nil && record.pidfd >= 0 {
		_ = a.backend.closePID(record.pidfd)
	}
}

func (a *localPluginAuthorizer) authorizeStart(fd int, pluginNo string, connection ...any) error {
	if pluginNo != searchSourcePluginNo {
		return errSearchSourceUnauthorized
	}
	timer := time.NewTimer(a.registrationWait)
	defer timer.Stop()
	for {
		peer, err := a.backend.peerCredentials(fd)
		if err != nil {
			return fmt.Errorf("read search source peer credentials: %w", err)
		}
		if peer.UID != a.euid {
			return errSearchSourceUnauthorized
		}

		a.mu.Lock()
		record := a.managed[peer.PID]
		if record != nil {
			if record.name != searchSourcePluginName || peer.UID != record.uid {
				a.mu.Unlock()
				return errSearchSourceUnauthorized
			}
			if err := a.validateManagedSearchProcess(record); err != nil {
				a.mu.Unlock()
				return errors.Join(errSearchSourceUnauthorized, err)
			}
			a.authorized[fd] = authorizedSearchConnection{pid: peer.PID, identity: searchConnectionIdentity(fd, connection)}
			a.mu.Unlock()
			return nil
		}
		if len(a.managed) != 0 {
			a.mu.Unlock()
			return errSearchSourceUnauthorized
		}
		managedChanged := a.managedChanged
		a.mu.Unlock()

		select {
		case <-managedChanged:
			// The parent registered a process. Re-read peer credentials before
			// matching the exact PID and validating its pidfd.
		case <-timer.C:
			return errSearchSourceUnauthorized
		}
	}
}

func (a *localPluginAuthorizer) authorizeRequest(fd int, pluginNo string, connection ...any) error {
	if pluginNo != searchSourcePluginNo {
		return errSearchSourceUnauthorized
	}
	peer, err := a.backend.peerCredentials(fd)
	if err != nil {
		return errors.Join(errSearchSourceUnauthorized, err)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	authorized, ok := a.authorized[fd]
	record := a.managed[authorized.pid]
	if !ok || authorized.pid != peer.PID || authorized.identity != searchConnectionIdentity(fd, connection) || record == nil || peer.UID != record.uid || peer.UID != a.euid {
		return errSearchSourceUnauthorized
	}
	if err := a.validateManagedSearchProcess(record); err != nil {
		return errors.Join(errSearchSourceUnauthorized, err)
	}
	return nil
}

func (a *localPluginAuthorizer) revokeConnection(fd int, connection ...any) bool {
	a.mu.Lock()
	authorized, ok := a.authorized[fd]
	if ok && authorized.identity == searchConnectionIdentity(fd, connection) {
		delete(a.authorized, fd)
	} else {
		ok = false
	}
	a.mu.Unlock()
	return ok
}

func (a *localPluginAuthorizer) waitForNoManagedSearchProcess(ctx context.Context) error {
	for {
		a.mu.Lock()
		if len(a.managed) == 0 {
			a.mu.Unlock()
			return nil
		}
		managedChanged := a.managedChanged
		a.mu.Unlock()
		select {
		case <-managedChanged:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (a *localPluginAuthorizer) unmanageSearchProcess(pid int) {
	a.mu.Lock()
	record := a.managed[pid]
	delete(a.managed, pid)
	for fd, authorized := range a.authorized {
		if authorized.pid == pid {
			delete(a.authorized, fd)
		}
	}
	if record != nil {
		a.signalManagedChangedLocked()
	}
	a.mu.Unlock()
	if record != nil {
		a.closeManagedSearchProcess(record)
	}
}

func (a *localPluginAuthorizer) close() {
	a.mu.Lock()
	records := make([]*managedSearchProcess, 0, len(a.managed))
	for _, record := range a.managed {
		records = append(records, record)
	}
	a.managed = make(map[int]*managedSearchProcess)
	a.authorized = make(map[int]authorizedSearchConnection)
	a.signalManagedChangedLocked()
	a.mu.Unlock()
	for _, record := range records {
		a.closeManagedSearchProcess(record)
	}
}

func searchConnectionIdentity(fd int, connection []any) any {
	if len(connection) != 0 {
		return connection[0]
	}
	return fd
}

func (a *localPluginAuthorizer) signalManagedChangedLocked() {
	close(a.managedChanged)
	a.managedChanged = make(chan struct{})
}
