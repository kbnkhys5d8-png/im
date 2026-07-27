package handler

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
)

func TestTrustedInternalConnectionRejectsSpoofedSystemDevice(t *testing.T) {
	previousOptions := options.G
	options.G = options.New()
	t.Cleanup(func() {
		options.G = previousOptions
	})

	spoofedClient := &eventbus.Conn{
		NodeId:   1001,
		ConnId:   42,
		DeviceId: options.G.SystemDeviceId,
	}
	if trustedInternalConnection(spoofedClient) {
		t.Fatal("a client-controlled system device id must not create a trusted internal connection")
	}

	internal := &eventbus.Conn{
		DeviceId: options.G.SystemDeviceId,
		Internal: true,
	}
	if !trustedInternalConnection(internal) {
		t.Fatal("an explicitly marked server-internal connection must be trusted")
	}
}
