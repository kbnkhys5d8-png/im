package event

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
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

		// 新 leader 不能信任源节点残留的认证结果；只在恢复时按设备核验当前凭据。
		// 沿用连接认证的适用范围，不改变管理连接、访客或未启用 Token 验证的行为。
		if options.G.TokenAuthOn && uid != options.G.ManagerUID && !options.G.IsVisitors(uid) {
			type credential struct {
				hash  string
				level wkproto.DeviceLevel
			}
			credentials := make(map[wkproto.DeviceFlag]credential)
			for _, conn := range prepared {
				current, loaded := credentials[conn.DeviceFlag]
				if !loaded {
					device, err := service.Store.GetDevice(uid, conn.DeviceFlag)
					if err != nil {
						return false
					}
					if device.Token != "" {
						current.hash = eventbus.TokenFingerprint(device.Token)
					}
					current.level = wkproto.DeviceLevel(device.DeviceLevel)
					credentials[conn.DeviceFlag] = current
				}
				// 等级也是认证结果；同 Token 改等级后不能恢复旧权限的会话。
				if current.hash == "" || conn.AuthTokenHash != current.hash || conn.DeviceLevel != current.level {
					conn.Revoked.Store(true)
				}
			}
		}

		p.Lock()
		defer p.Unlock()
		if p.handler(uid) != h {
			return false
		}
		h.pending.RLock()
		defer h.pending.RUnlock()
		// 凭据查询期间排队或已经完成的 Token 任务，都使本次核验结果失效。
		// 持读锁到应用完成，保证后续任务能看到已恢复的连接再执行撤销。
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
					if incoming.Revoked.Load() {
						current.Revoked.Store(true)
					}
					delete(prepared, key)
				}
				next = append(next, current)
				continue
			}
			if found {
				if current.SameSession(incoming) {
					// 同代保留原撤销引用，不能让已持有旧指针的撤销写入失效。
					if incoming.Revoked.Load() {
						current.Revoked.Store(true)
					}
					current.LastActive = incoming.LastActive
					next = append(next, current)
				} else if !incoming.Revoked.Load() {
					// 新代际独立验证，不继承旧连接号的撤销状态。
					next = append(next, incoming)
				}
				delete(prepared, key)
			}
		}
		for _, incoming := range prepared {
			if !incoming.Revoked.Load() {
				next = append(next, incoming)
			}
		}
		h.conns.conns = next
		h.conns.revision++
		h.conns.hasRecovered = true
		h.conns.recoveredVersion = version
		return true
	}, true
}

func cloneRecoveryConn(conn *eventbus.Conn) *eventbus.Conn {
	cloned := &eventbus.Conn{
		Uid: conn.Uid, NodeId: conn.NodeId, ConnId: conn.ConnId,
		SessionId: conn.SessionId, DeviceId: conn.DeviceId,
		AuthTokenHash: conn.AuthTokenHash,
		DeviceFlag:    conn.DeviceFlag, DeviceLevel: conn.DeviceLevel,
		Auth: conn.Auth, Internal: conn.Internal,
		AesIV:        append([]byte(nil), conn.AesIV...),
		AesKey:       append([]byte(nil), conn.AesKey...),
		ProtoVersion: conn.ProtoVersion, Uptime: conn.Uptime,
		LastActive: fasttime.UnixTimestamp(), IsJsonRpc: conn.IsJsonRpc,
	}
	cloned.Revoked.Store(conn.Revoked.Load())
	return cloned
}
