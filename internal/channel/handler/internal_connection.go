package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
)

func trustedInternalConnection(conn *eventbus.Conn) bool {
	if conn == nil || !options.G.IsSystemDevice(conn.DeviceId) {
		return false
	}
	if conn.Internal {
		return true
	}

	// Compatibility for internal events emitted by a node running the previous
	// version during a rolling upgrade. Real client connections always receive
	// a non-zero node ID and connection ID from the server.
	return conn.NodeId == 0 && conn.ConnId == 0
}
