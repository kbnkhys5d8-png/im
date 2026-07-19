package plugin

import (
	"sort"
	"sync"

	"github.com/panjf2000/gnet/v2"
)

type pluginManager struct {
	plugins []*Plugin
	sync.RWMutex
}

func newPluginManager() *pluginManager {

	return &pluginManager{}
}

func (pm *pluginManager) add(p *Plugin) {
	pm.Lock()
	defer pm.Unlock()
	exist := false
	for i, v := range pm.plugins {
		if v.info.No == p.info.No {
			pm.plugins[i] = p
			exist = true
			break
		}
	}
	if !exist {
		pm.plugins = append(pm.plugins, p)
	}

	sort.Slice(pm.plugins, func(i, j int) bool {
		return pm.plugins[i].info.Priority < pm.plugins[j].info.Priority
	})
}

func (pm *pluginManager) remove(no string) {
	pm.Lock()
	defer pm.Unlock()
	for i, v := range pm.plugins {
		if v.info.No == no {
			pm.plugins = append(pm.plugins[:i], pm.plugins[i+1:]...)
			return
		}
	}
}

func (pm *pluginManager) removeIf(no string, conn gnet.Conn) bool {
	pm.Lock()
	defer pm.Unlock()
	for i, plugin := range pm.plugins {
		if plugin.info.No == no && plugin.conn == conn {
			pm.plugins = append(pm.plugins[:i], pm.plugins[i+1:]...)
			return true
		}
	}
	return false
}

func (pm *pluginManager) get(no string) *Plugin {
	pm.RLock()
	defer pm.RUnlock()
	for _, v := range pm.plugins {
		if v.info.No == no {
			return v
		}
	}
	return nil
}

func (pm *pluginManager) getByName(name string) []*Plugin {
	pm.RLock()
	defer pm.RUnlock()
	var plugins []*Plugin
	for _, v := range pm.plugins {
		if v.info.Name == name {
			plugins = append(plugins, v)
		}
	}
	return plugins
}

func (pm *pluginManager) all() []*Plugin {
	pm.RLock()
	defer pm.RUnlock()
	return append([]*Plugin(nil), pm.plugins...)
}
