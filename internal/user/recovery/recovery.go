package recovery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/fasttime"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"golang.org/x/sync/errgroup"
)

const (
	requestPath = "/wk/ingress/recoverUserConnections"
	resultPath  = "/wk/ingress/recoveredUserConnections"
	refreshPath = "/wk/ingress/refreshAuthenticatedConnection"
	maxBodySize = 1 << 20
	timeout     = 5 * time.Second
)

var errChanged = errors.New("connection recovery state changed")

type directory interface {
	BeginConnRecovery(uid string, version uint64) (func([]*eventbus.Conn) bool, bool, error)
	LocalConnByUid(uid string) []*eventbus.Conn
}

type refreshDirectory interface {
	InvalidateConnRecovery(*eventbus.Conn) bool
	ConnById(string, uint64, int64) *eventbus.Conn
}

// 刷新请求只携带会话定位信息，不携带认证断言或 AES 密钥。
type refreshRequest struct {
	UID       string `json:"uid"`
	Owner     uint64 `json:"owner"`
	ConnID    int64  `json:"conn_id"`
	SessionID string `json:"session_id"`
	Leader    uint64 `json:"leader"`
	Version   uint64 `json:"version"`
}

type request struct {
	UID     string `json:"uid"`
	Leader  uint64 `json:"leader"`
	Version uint64 `json:"version"`
	Nonce   string `json:"nonce"`
}

type result struct {
	request
	Owner uint64   `json:"owner"`
	Conns [][]byte `json:"conns"`
}

type pending struct {
	request
	owner    uint64
	deadline time.Time
	result   chan []*eventbus.Conn
}

type flight struct {
	done chan struct{}
	err  error
}

// Recovery 只通过配置中的出站集群连接交换快照，不向入站请求者导出会话密钥。
// 该握手不替代集群网络隔离或传输加密，也不改变客户端的认证与加密协议。
type Recovery struct {
	nodeID  uint64
	cluster icluster.ICluster
	users   directory
	conns   service.IConnManager
	mu      sync.Mutex
	pending map[string]*pending
	flights map[string]*flight
	queries chan struct{}
}

func New(nodeID uint64, cluster icluster.ICluster, users directory, conns service.IConnManager) *Recovery {
	return &Recovery{
		nodeID: nodeID, cluster: cluster, users: users, conns: conns,
		pending: make(map[string]*pending), flights: make(map[string]*flight),
		queries: make(chan struct{}, 16),
	}
}

func (r *Recovery) SetRoutes() {
	r.cluster.Route(refreshPath, func(c *wkserver.Context) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := r.refresh(ctx, c.Body()); err != nil {
			c.WriteErr(err)
			return
		}
		c.WriteOk()
	})
	r.cluster.Route(requestPath, func(c *wkserver.Context) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := r.respond(ctx, c.Body()); err != nil {
			c.WriteErr(err)
			return
		}
		// 密钥仅回送到配置中的领导节点；原请求只收到成功状态。
		c.WriteOk()
	})
	r.cluster.Route(resultPath, func(c *wkserver.Context) {
		if err := r.receive(c.Body()); err != nil {
			c.WriteErr(err)
			return
		}
		c.WriteOk()
	})
}

func (r *Recovery) Ensure(ctx context.Context, uid string) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		if running := r.flights[uid]; running != nil {
			r.mu.Unlock()
			select {
			case <-running.done:
				if running.err != nil {
					return running.err
				}
				// 等待期间标记可能再次失效，不能沿用旧 flight 的成功结果。
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		running := &flight{done: make(chan struct{})}
		r.flights[uid] = running
		r.mu.Unlock()

		err := r.ensure(ctx, uid)
		r.mu.Lock()
		running.err = err
		delete(r.flights, uid)
		close(running.done)
		r.mu.Unlock()
		return err
	}
}

// ConfirmAuthenticated 仅给跨换主完成的认证补登；普通登录不增加网络往返。
func (r *Recovery) ConfirmAuthenticated(ctx context.Context, conn *eventbus.Conn, authLeader uint64) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !r.liveAuthenticatedSession(conn) {
			return errors.New("authenticated owner session no longer exists")
		}
		version := r.cluster.NodeVersion()
		leader := r.cluster.SlotLeaderId(r.cluster.GetSlotId(conn.Uid))
		if !r.currentLeader(conn.Uid, leader, version) {
			lastErr = errChanged
		} else if leader == r.nodeID || (leader == authLeader && conn.AdmissionVersion != 0 && conn.AdmissionVersion == version) {
			return nil
		} else {
			body, err := json.Marshal(refreshRequest{
				UID: conn.Uid, Owner: r.nodeID, ConnID: conn.ConnId,
				SessionID: conn.SessionId, Leader: leader, Version: version,
			})
			if err != nil {
				return err
			}
			resp, err := r.cluster.RequestWithContext(ctx, leader, refreshPath, body)
			if err == nil && resp != nil && resp.Status == proto.StatusOK &&
				ctx.Err() == nil && r.currentLeader(conn.Uid, leader, version) && r.liveAuthenticatedSession(conn) {
				return nil
			}
			lastErr = err
			if lastErr == nil {
				lastErr = errors.New("current leader did not confirm authenticated session")
			}
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (r *Recovery) liveAuthenticatedSession(conn *eventbus.Conn) bool {
	if conn == nil || !validSnapshotConn(conn, conn.Uid, r.nodeID) {
		return false
	}
	real := r.conns.GetConn(conn.ConnId)
	if real == nil || real.IsClosed() {
		return false
	}
	current, ok := real.Context().(*eventbus.Conn)
	return ok && validSnapshotConn(current, conn.Uid, r.nodeID) && current.SameSession(conn)
}

func (r *Recovery) refresh(ctx context.Context, body []byte) error {
	var req refreshRequest
	if len(body) > maxBodySize || json.Unmarshal(body, &req) != nil || req.ConnID <= 0 ||
		req.SessionID == "" || len(req.SessionID) > 128 || req.Owner == 0 {
		return errors.New("invalid authenticated connection refresh")
	}
	if req.Leader != r.nodeID || !r.currentLeader(req.UID, r.nodeID, req.Version) ||
		r.cluster.NodeInfoById(req.Owner) == nil || !r.cluster.NodeIsOnline(req.Owner) {
		return errChanged
	}
	users, ok := r.users.(refreshDirectory)
	if !ok {
		return errors.New("connection refresh unsupported")
	}
	// Auth 仅满足失效入口的过滤条件；传入身份绝不直接加入投递目录。
	users.InvalidateConnRecovery(&eventbus.Conn{
		Uid: req.UID, NodeId: req.Owner, ConnId: req.ConnID, SessionId: req.SessionID, Auth: true,
	})
	if err := r.Ensure(ctx, req.UID); err != nil {
		return err
	}
	conn := users.ConnById(req.UID, req.Owner, req.ConnID)
	if ctx.Err() != nil || !r.currentLeader(req.UID, r.nodeID, req.Version) ||
		!validSnapshotConn(conn, req.UID, req.Owner) || conn.SessionId != req.SessionID {
		return errors.New("authenticated session missing from verified snapshot")
	}
	return nil
}

func (r *Recovery) ensure(ctx context.Context, uid string) error {
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		version := r.cluster.NodeVersion()
		if !r.currentLeader(uid, r.nodeID, version) {
			return errChanged
		}
		apply, needed, err := r.users.BeginConnRecovery(uid, version)
		if err != nil || !needed {
			return err
		}
		conns, err := r.fetch(ctx, uid, version)
		if err == nil && ctx.Err() == nil && r.currentLeader(uid, r.nodeID, version) && apply(conns) {
			return nil
		}
		lastErr = err
		if lastErr == nil {
			lastErr = errChanged
		}
		// 关闭、重连或配置传播可能使快照失效；有界重取，不强行覆盖新状态。
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return lastErr
}

func (r *Recovery) currentLeader(uid string, leader, version uint64) bool {
	return uid != "" && len(uid) <= 4096 && leader != 0 &&
		r.cluster.NodeVersion() == version &&
		r.cluster.SlotLeaderId(r.cluster.GetSlotId(uid)) == leader &&
		r.cluster.NodeInfoById(leader) != nil && r.cluster.NodeIsOnline(leader)
}

func (r *Recovery) fetch(ctx context.Context, uid string, version uint64) ([]*eventbus.Conn, error) {
	var mu sync.Mutex
	var all []*eventbus.Conn
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for _, node := range r.cluster.Nodes() {
		if !node.Online {
			continue
		}
		nodeID := node.Id
		group.Go(func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			var conns []*eventbus.Conn
			var err error
			if nodeID == r.nodeID {
				conns, err = r.localSnapshot(uid)
			} else {
				conns, err = r.query(ctx, nodeID, uid, version)
			}
			if err != nil {
				return err
			}
			mu.Lock()
			all = append(all, conns...)
			mu.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	return all, nil
}

func (r *Recovery) query(ctx context.Context, owner uint64, uid string, version uint64) ([]*eventbus.Conn, error) {
	// 不同群和用户共享上限，避免换主时同时向接入节点发起无界请求。
	select {
	case r.queries <- struct{}{}:
		defer func() { <-r.queries }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	var secret [32]byte
	if _, err := rand.Read(secret[:]); err != nil {
		return nil, err
	}
	req := request{UID: uid, Leader: r.nodeID, Version: version, Nonce: hex.EncodeToString(secret[:])}
	deadline, _ := ctx.Deadline()
	p := &pending{request: req, owner: owner, deadline: deadline, result: make(chan []*eventbus.Conn, 1)}
	r.mu.Lock()
	r.pending[req.Nonce] = p
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.pending, req.Nonce)
		r.mu.Unlock()
	}()
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := r.cluster.RequestWithContext(ctx, owner, requestPath, body)
	if err != nil {
		return nil, err
	}
	if resp == nil || resp.Status != proto.StatusOK {
		return nil, fmt.Errorf("connection recovery request failed on node %d", owner)
	}
	select {
	case conns := <-p.result:
		return conns, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (r *Recovery) respond(ctx context.Context, body []byte) error {
	var req request
	if len(body) > maxBodySize || json.Unmarshal(body, &req) != nil || len(req.Nonce) != 64 {
		return errors.New("invalid connection recovery request")
	}
	if _, err := hex.DecodeString(req.Nonce); err != nil || !r.currentLeader(req.UID, req.Leader, req.Version) {
		return errChanged
	}
	conns, err := r.localSnapshot(req.UID)
	if err != nil {
		return err
	}
	res := result{request: req, Owner: r.nodeID}
	for _, conn := range conns {
		encoded, err := conn.Encode()
		if err != nil {
			return err
		}
		res.Conns = append(res.Conns, encoded)
	}
	encoded, err := json.Marshal(res)
	if err != nil {
		return err
	}
	if len(encoded) > maxBodySize || !r.currentLeader(req.UID, req.Leader, req.Version) {
		return errChanged
	}
	// 不使用入站连接的自报节点号或地址作为回传目标。
	resp, err := r.cluster.RequestWithContext(ctx, req.Leader, resultPath, encoded)
	if err != nil {
		return err
	}
	if resp == nil || resp.Status != proto.StatusOK {
		return errors.New("connection recovery callback rejected")
	}
	return nil
}

func (r *Recovery) receive(body []byte) error {
	var res result
	if len(body) > maxBodySize || json.Unmarshal(body, &res) != nil {
		return errors.New("invalid connection recovery result")
	}
	r.mu.Lock()
	p := r.pending[res.Nonce]
	r.mu.Unlock()
	if p == nil || p.request != res.request || p.owner != res.Owner || time.Now().After(p.deadline) ||
		!r.currentLeader(res.UID, r.nodeID, res.Version) || !r.cluster.NodeIsOnline(res.Owner) {
		return errors.New("unsolicited or stale connection recovery result")
	}
	conns := make([]*eventbus.Conn, 0, len(res.Conns))
	seen := make(map[int64]struct{}, len(res.Conns))
	for _, data := range res.Conns {
		conn := &eventbus.Conn{}
		if conn.Decode(data) != nil || !validSnapshotConn(conn, res.UID, res.Owner) {
			return errors.New("invalid recovered connection")
		}
		if _, exists := seen[conn.ConnId]; exists {
			return errors.New("duplicate recovered connection")
		}
		seen[conn.ConnId] = struct{}{}
		conn.LastActive = fasttime.UnixTimestamp()
		conns = append(conns, conn)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pending[res.Nonce] != p || time.Now().After(p.deadline) {
		return errors.New("expired connection recovery result")
	}
	delete(r.pending, res.Nonce)
	p.result <- conns
	return nil
}

func validSnapshotConn(conn *eventbus.Conn, uid string, owner uint64) bool {
	return conn != nil && conn.Uid == uid && conn.NodeId == owner && conn.ConnId > 0 &&
		conn.Auth && !conn.Internal && conn.SessionId != ""
}

func (r *Recovery) localSnapshot(uid string) ([]*eventbus.Conn, error) {
	var result []*eventbus.Conn
	for _, candidate := range r.users.LocalConnByUid(uid) {
		if candidate == nil || candidate.ConnId <= 0 || candidate.NodeId != r.nodeID {
			continue
		}
		real := r.conns.GetConn(candidate.ConnId)
		if real == nil || real.IsClosed() {
			continue
		}
		conn, ok := real.Context().(*eventbus.Conn)
		if ok && conn != nil && conn.Uid == uid && conn.NodeId == r.nodeID && conn.Auth && !conn.Internal && conn.SessionId == "" {
			// 混合旧版本可能丢失内部代次，不能把尚不能安全恢复的在线连接判为离线。
			return nil, errors.New("live connection lacks recovery session identity")
		}
		if !ok || !validSnapshotConn(conn, uid, r.nodeID) || !candidate.SameSession(conn) {
			continue
		}
		// 仅复制已发布的认证字段；统计原子量与活动时间不通过结构体复制。
		encoded, err := conn.Encode()
		if err != nil {
			return nil, err
		}
		copy := &eventbus.Conn{}
		if err := copy.Decode(encoded); err != nil {
			return nil, err
		}
		copy.LastActive = fasttime.UnixTimestamp()
		if !real.IsClosed() && r.conns.GetConn(conn.ConnId) == real {
			result = append(result, copy)
		}
	}
	return result, nil
}
