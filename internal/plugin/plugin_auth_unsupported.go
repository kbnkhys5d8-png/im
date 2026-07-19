//go:build !linux

package plugin

import "errors"

type unsupportedLocalAuthBackend struct{}

func newLocalAuthBackend() localAuthBackend { return unsupportedLocalAuthBackend{} }

func (unsupportedLocalAuthBackend) peerCredentials(int) (localPeerCredentials, error) {
	return localPeerCredentials{}, errors.New("SO_PEERCRED search source authentication requires linux")
}

func (unsupportedLocalAuthBackend) openPID(int) (int, error) {
	return -1, errors.New("pidfd search source authentication requires linux")
}

func (unsupportedLocalAuthBackend) pidAlive(int) error {
	return errors.New("pidfd search source authentication requires linux")
}

func (unsupportedLocalAuthBackend) pidProcessAlive(int) error {
	return errors.New("process search source authentication requires linux")
}

func (unsupportedLocalAuthBackend) processIdentity(int) (localProcessIdentity, error) {
	return localProcessIdentity{}, errors.New("process search source authentication requires linux")
}

func (unsupportedLocalAuthBackend) closePID(int) error { return nil }
