package plugin

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/panjf2000/gnet/v2"
)

func TestPluginManagerStaleCloseCannotRemoveReplacement(t *testing.T) {
	manager := newPluginManager()
	oldConn := &pluginManagerTestConn{}
	newConn := &pluginManagerTestConn{}
	oldPlugin := &Plugin{conn: oldConn, info: &pluginproto.PluginInfo{No: searchSourcePluginNo}}
	newPlugin := &Plugin{conn: newConn, info: &pluginproto.PluginInfo{No: searchSourcePluginNo}}
	manager.add(oldPlugin)
	manager.add(newPlugin)

	if manager.removeIf(searchSourcePluginNo, oldConn) {
		t.Fatal("stale close reported that it removed the replacement plugin")
	}
	if got := manager.get(searchSourcePluginNo); got != newPlugin {
		t.Fatal("stale close removed the replacement plugin")
	}
	if !manager.removeIf(searchSourcePluginNo, newConn) {
		t.Fatal("current connection could not remove its own plugin")
	}
	if got := manager.get(searchSourcePluginNo); got != nil {
		t.Fatal("current plugin remained registered after its own close")
	}
}

type pluginManagerTestConn struct{ gnet.Conn }
