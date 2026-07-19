//go:build linux

package plugin

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

type linuxLocalAuthBackend struct{}

func newLocalAuthBackend() localAuthBackend { return linuxLocalAuthBackend{} }

func (linuxLocalAuthBackend) peerCredentials(fd int) (localPeerCredentials, error) {
	credentials, err := unix.GetsockoptUcred(fd, unix.SOL_SOCKET, unix.SO_PEERCRED)
	if err != nil {
		return localPeerCredentials{}, err
	}
	return localPeerCredentials{PID: int(credentials.Pid), UID: credentials.Uid}, nil
}

func (linuxLocalAuthBackend) openPID(pid int) (int, error) {
	pidfd, err := unix.PidfdOpen(pid, 0)
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) {
		return -1, errors.Join(errPIDFDUnsupported, err)
	}
	return pidfd, err
}

func (linuxLocalAuthBackend) pidAlive(pidfd int) error {
	err := unix.PidfdSendSignal(pidfd, 0, nil, 0)
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EPERM) {
		return errors.Join(errPIDFDUnsupported, err)
	}
	return err
}

func (linuxLocalAuthBackend) pidProcessAlive(pid int) error { return unix.Kill(pid, 0) }

func (linuxLocalAuthBackend) processIdentity(pid int) (localProcessIdentity, error) {
	procDir := fmt.Sprintf("/proc/%d", pid)
	info, err := os.Stat(procDir)
	if err != nil {
		return localProcessIdentity{}, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return localProcessIdentity{}, errors.New("read /proc owner")
	}
	data, err := os.ReadFile(procDir + "/stat")
	if err != nil {
		return localProcessIdentity{}, err
	}
	openParen := bytes.IndexByte(data, '(')
	closeParen := bytes.LastIndexByte(data, ')')
	if openParen <= 0 || closeParen <= openParen {
		return localProcessIdentity{}, errors.New("parse /proc stat command")
	}
	statPID, err := strconv.Atoi(strings.TrimSpace(string(data[:openParen])))
	if err != nil || statPID != pid {
		return localProcessIdentity{}, errors.New("parse /proc stat pid")
	}
	fields := strings.Fields(string(data[closeParen+1:]))
	if len(fields) <= 19 {
		return localProcessIdentity{}, errors.New("parse /proc stat starttime")
	}
	startTime, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || startTime == 0 {
		return localProcessIdentity{}, errors.New("parse /proc stat starttime")
	}
	return localProcessIdentity{PID: statPID, UID: stat.Uid, StartTime: startTime}, nil
}

func (linuxLocalAuthBackend) closePID(pidfd int) error { return unix.Close(pidfd) }
