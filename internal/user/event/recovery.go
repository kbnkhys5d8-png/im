package event

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
)

type recoveryConnKey struct {
	nodeId uint64
	connId int64
}

// InvalidateConnRecovery 只失效恢复缓存；新身份仍须从 owner 的真实 socket 验证。
func (e *EventPool) InvalidateConnRecovery(conn *eventbus.Conn) bool {
	if conn == nil || conn.Uid == "" || conn.NodeId == 0 || conn.ConnId <= 0 ||
		!conn.Auth || conn.Internal || conn.SessionId == "" {
		return false
	}
	p := e.pollerByUid(conn.Uid)
	p.Lock()
	defer p.Unlock()
	h := p.handler(conn.Uid)
	if h == nil {
		return true
	}
	h.conns.Lock()
	defer h.conns.Unlock()
	for _, current := range h.conns.conns {
		if current.NodeId == conn.NodeId && current.ConnId == conn.ConnId &&
			current.Auth && !current.Internal && current.SessionId == conn.SessionId {
			return false
		}
	}
	// 即便此前已经失效，也要拒绝本次提示之前发起的旧快照。
	h.conns.hasRecovered = false
	h.conns.revision++
	return true
}

// BeginConnRecovery 捕获用户目录代次；远程查询由调用方在锁外执行。
func (e *EventPool) BeginConnRecovery(uid string, version uint64) (func([]*eventbus.Conn) bool, bool) {
	p := e.pollerByUid(uid)
	p.Lock()
	h := p.handler(uid)
	if h == nil {
		h = newUserHandler(uid, p)
		p.waitlist.push(h)
	}
	h.pending.RLock()
	pendingRevision := h.pending.connRevision
	pendingCount := h.pending.connEvents
	h.conns.RLock()
	directoryRevision := h.conns.revision
	recoveredVersion := h.conns.recoveredVersion
	hasRecovered := h.conns.hasRecovered
	h.conns.RUnlock()
	h.pending.RUnlock()
	p.Unlock()

	if hasRecovered && recoveredVersion == version {
		return nil, false
	}
	// 已经应用过更高版本，旧请求不允许回滚目录。
	if pendingCount != 0 || recoveredVersion > version {
		return func([]*eventbus.Conn) bool { return false }, true
	}
	return func(snapshot []*eventbus.Conn) bool {
		prepared := make(map[recoveryConnKey]*eventbus.Conn, len(snapshot))
		for _, conn := range snapshot {
			if conn == nil || conn.Uid != uid || conn.NodeId == 0 || conn.ConnId <= 0 ||
				!conn.Auth || conn.Internal || conn.SessionId == "" {
				return false
			}
			key := recoveryConnKey{nodeId: conn.NodeId, connId: conn.ConnId}
			if _, exists := prepared[key]; exists {
				return false
			}
			prepared[key] = cloneRecoveryConn(conn)
		}

		p.Lock()
		defer p.Unlock()
		if p.handler(uid) != h {
			return false
		}
		h.pending.RLock()
		defer h.pending.RUnlock()
		if h.pending.connRevision != pendingRevision || h.pending.connEvents != 0 {
			return false
		}
		h.conns.Lock()
		defer h.conns.Unlock()
		if h.conns.revision != directoryRevision || h.conns.recoveredVersion > version ||
			(h.conns.hasRecovered && h.conns.recoveredVersion == version) {
			return false
		}

		next := make([]*eventbus.Conn, 0, len(h.conns.conns)+len(prepared))
		for _, current := range h.conns.conns {
			key := recoveryConnKey{nodeId: current.NodeId, connId: current.ConnId}
			incoming, found := prepared[key]
			if options.G.IsLocalNode(current.NodeId) || current.Internal || !current.Auth {
				if found {
					// 本地 socket 的身份只能由本地连接管理器改变；不覆盖真实指针。
					if !current.Auth || current.Internal || !current.SameSession(incoming) {
						return false
					}
					delete(prepared, key)
				}
				next = append(next, current)
				continue
			}
			if found {
				// 远端记录以本次真实 socket 快照为准，同时刷新活跃时间。
				next = append(next, incoming)
				delete(prepared, key)
			}
		}
		for _, incoming := range prepared {
			next = append(next, incoming)
		}
		h.conns.conns = next
		h.conns.revision++
		h.conns.hasRecovered = true
		h.conns.recoveredVersion = version
		return true
	}, true
}

func cloneRecoveryConn(conn *eventbus.Conn) *eventbus.Conn {
	return &eventbus.Conn{
		Uid: conn.Uid, NodeId: conn.NodeId, ConnId: conn.ConnId,
		SessionId: conn.SessionId, DeviceId: conn.DeviceId,
		DeviceFlag: conn.DeviceFlag, DeviceLevel: conn.DeviceLevel,
		Auth: conn.Auth, Internal: conn.Internal,
		AesIV:        append([]byte(nil), conn.AesIV...),
		AesKey:       append([]byte(nil), conn.AesKey...),
		ProtoVersion: conn.ProtoVersion, Uptime: conn.Uptime,
		LastActive: fasttime.UnixTimestamp(), IsJsonRpc: conn.IsJsonRpc,
	}
}
