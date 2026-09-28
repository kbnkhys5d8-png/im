package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
)

// 仅覆盖恢复协议用到的接口；测试不创建监听器或真实网络连接。
type recoveryCluster struct {
	icluster.ICluster
	leader  uint64
	version uint64
	nodes   []*types.Node
	routes  map[string]wkserver.Handler
	request func(context.Context, uint64, string, []byte) (*proto.Response, error)
}

func newRecoveryCluster() *recoveryCluster {
	return &recoveryCluster{
		leader: 1, version: 7,
		nodes:  []*types.Node{{Id: 1, Online: true}, {Id: 2, Online: true}},
		routes: make(map[string]wkserver.Handler),
	}
}

func (c *recoveryCluster) GetSlotId(string) uint32            { return 4 }
func (c *recoveryCluster) SlotLeaderId(uint32) uint64         { return c.leader }
func (c *recoveryCluster) NodeVersion() uint64                { return c.version }
func (c *recoveryCluster) Nodes() []*types.Node               { return c.nodes }
func (c *recoveryCluster) Route(p string, h wkserver.Handler) { c.routes[p] = h }

func (c *recoveryCluster) NodeInfoById(id uint64) *types.Node {
	for _, node := range c.nodes {
		if node.Id == id {
			return node
		}
	}
	return nil
}

func (c *recoveryCluster) NodeIsOnline(id uint64) bool {
	node := c.NodeInfoById(id)
	return node != nil && node.Online
}

func (c *recoveryCluster) RequestWithContext(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	if c.request == nil {
		return nil, errors.New("unexpected recovery request")
	}
	return c.request(ctx, node, path, body)
}

type recoveryDirectory struct {
	mu              sync.Mutex
	local           []*eventbus.Conn
	beginCalls      int
	applyCalls      int
	rejectRemaining int
	recovered       bool
	applied         []*eventbus.Conn
}

func (d *recoveryDirectory) LocalConnByUid(string) []*eventbus.Conn {
	return d.local
}

func (d *recoveryDirectory) BeginConnRecovery(string, uint64) (func([]*eventbus.Conn) bool, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.beginCalls++
	if d.recovered {
		return nil, false, nil
	}
	return func(conns []*eventbus.Conn) bool {
		d.mu.Lock()
		defer d.mu.Unlock()
		d.applyCalls++
		if d.rejectRemaining > 0 {
			d.rejectRemaining--
			return false
		}
		d.applied = conns
		d.recovered = true
		return true
	}, true, nil
}

func (d *recoveryDirectory) counts() (int, int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.beginCalls, d.applyCalls
}

type recoveryConnManager struct {
	service.IConnManager
	conn wknet.Conn
	get  func(int64) wknet.Conn
}

func (m *recoveryConnManager) GetConn(id int64) wknet.Conn {
	if m.get != nil {
		return m.get(id)
	}
	return m.conn
}

type recoverySocket struct {
	wknet.Conn
	context  any
	closed   bool
	isClosed func() bool
}

func (c *recoverySocket) Context() any { return c.context }
func (c *recoverySocket) IsClosed() bool {
	if c.isClosed != nil {
		return c.isClosed()
	}
	return c.closed
}

func recoveryTestConn() *eventbus.Conn {
	return &eventbus.Conn{
		Uid: "alice", NodeId: 2, ConnId: 23, SessionId: "owner-session-23",
		DeviceId: "phone", DeviceFlag: 1, DeviceLevel: 1, Auth: true,
		AesIV: []byte("0123456789abcdef"), AesKey: []byte("abcdef0123456789"),
		ProtoVersion: 4, Uptime: 100, LastActive: 10,
	}
}

func encodeRecoveryConn(t *testing.T, conn *eventbus.Conn) []byte {
	t.Helper()
	data, err := conn.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func recoveryJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func recoveryContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitRecoveryResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("connection recovery did not finish")
		return nil
	}
}

func assertRecoveryIdle(t *testing.T, r *Recovery) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) != 0 || len(r.flights) != 0 || len(r.queries) != 0 {
		t.Fatalf("recovery state leaked: pending=%d flights=%d queries=%d", len(r.pending), len(r.flights), len(r.queries))
	}
}

func TestRecoverySetRoutes(t *testing.T) {
	cluster := newRecoveryCluster()
	r := New(1, cluster, &recoveryDirectory{}, &recoveryConnManager{})
	r.SetRoutes()
	if len(cluster.routes) != 3 || cluster.routes[requestPath] == nil || cluster.routes[resultPath] == nil || cluster.routes[refreshPath] == nil {
		t.Fatalf("unexpected recovery routes: %v", cluster.routes)
	}
}

func TestRecoveryEnsurePartialOwnerFailureDoesNotApply(t *testing.T) {
	cluster := newRecoveryCluster()
	cluster.nodes = append(cluster.nodes, &types.Node{Id: 3, Online: true})
	directory := &recoveryDirectory{}
	r := New(1, cluster, directory, &recoveryConnManager{})
	ownerFailure := errors.New("owner 3 unavailable")
	ownerSucceeded := make(chan struct{}, 3)
	var successes atomic.Int32
	encoded := encodeRecoveryConn(t, recoveryTestConn())
	cluster.request = func(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
		if node == 3 {
			select {
			case <-ownerSucceeded:
				return nil, ownerFailure
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		var req request
		if node != 2 || path != requestPath || json.Unmarshal(body, &req) != nil {
			return nil, errors.New("unexpected owner request")
		}
		data, err := json.Marshal(result{request: req, Owner: node, Conns: [][]byte{encoded}})
		if err != nil {
			return nil, err
		}
		if err := r.receive(data); err != nil {
			return nil, err
		}
		successes.Add(1)
		ownerSucceeded <- struct{}{}
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	if err := r.Ensure(recoveryContext(t), "alice"); !errors.Is(err, ownerFailure) {
		t.Fatalf("got %v, want owner failure", err)
	}
	if _, applied := directory.counts(); applied != 0 || successes.Load() != 3 {
		t.Fatalf("partial snapshot applied=%d, successful owner attempts=%d", applied, successes.Load())
	}
	assertRecoveryIdle(t, r)
}

func TestRecoveryEnsureCoalescesAndWaiterCanCancel(t *testing.T) {
	cluster := newRecoveryCluster()
	directory := &recoveryDirectory{}
	r := New(1, cluster, directory, &recoveryConnManager{})
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	cluster.request = func(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		var req request
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, err
		}
		data, _ := json.Marshal(result{request: req, Owner: node})
		if err := r.receive(data); err != nil {
			return nil, err
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	ctx := recoveryContext(t)
	primary := make(chan error, 1)
	go func() { primary <- r.Ensure(ctx, "alice") }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("primary recovery did not start")
	}
	// 首次恢复被明确阻塞，活跃的等待者超时前必须复用它，不能另启目录恢复。
	waiterCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if err := r.Ensure(waiterCtx, "alice"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter cancellation: %v", err)
	}
	if begun, applied := directory.counts(); begun != 1 || applied != 0 || calls.Load() != 1 {
		t.Fatalf("duplicate recovery: begin=%d apply=%d requests=%d", begun, applied, calls.Load())
	}
	close(release)
	if err := waitRecoveryResult(t, primary); err != nil {
		t.Fatal(err)
	}
	if _, applied := directory.counts(); applied != 1 {
		t.Fatalf("successful recovery applied %d times", applied)
	}
	assertRecoveryIdle(t, r)
}

func TestRecoveryJoinedSuccessRechecksInvalidatedDirectory(t *testing.T) {
	cluster := newRecoveryCluster()
	directory := &recoveryDirectory{}
	r := New(1, cluster, directory, &recoveryConnManager{})
	old := &flight{done: make(chan struct{})}
	r.flights["alice"] = old
	cluster.request = func(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
		var req request
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, err
		}
		data, err := json.Marshal(result{request: req, Owner: node})
		if err != nil {
			return nil, err
		}
		if err := r.receive(data); err != nil {
			return nil, err
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	ctx := recoveryContext(t)
	done := make(chan error, 1)
	go func() { done <- r.Ensure(ctx, "alice") }()
	// 无缓冲握手精确证明调用者已加入旧 flight，不依赖 sleep 或调度时机。
	select {
	case old.done <- struct{}{}:
	case <-ctx.Done():
		t.Fatal("等待者没有加入旧 flight")
	}
	r.mu.Lock()
	delete(r.flights, "alice")
	close(old.done)
	r.mu.Unlock()
	if err := waitRecoveryResult(t, done); err != nil {
		t.Fatal(err)
	}
	if _, applied := directory.counts(); applied != 1 {
		t.Fatalf("旧 flight 成功后仍须恢复已失效目录，实际应用次数=%d", applied)
	}
}

func TestRecoveryEnsureCancellationCleansPending(t *testing.T) {
	for _, stage := range []string{"request", "callback"} {
		t.Run(stage, func(t *testing.T) {
			cluster := newRecoveryCluster()
			directory := &recoveryDirectory{}
			r := New(1, cluster, directory, &recoveryConnManager{})
			started := make(chan struct{})
			cluster.request = func(ctx context.Context, _ uint64, _ string, _ []byte) (*proto.Response, error) {
				close(started)
				if stage == "request" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return &proto.Response{Status: proto.StatusOK}, nil
			}
			ctx, cancel := context.WithCancel(recoveryContext(t))
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- r.Ensure(ctx, "alice") }()
			select {
			case <-started:
			case <-ctx.Done():
				t.Fatal("recovery did not reach cancellation point")
			}
			cancel()
			if err := waitRecoveryResult(t, done); !errors.Is(err, context.Canceled) {
				t.Fatalf("got %v, want cancellation", err)
			}
			if _, applied := directory.counts(); applied != 0 {
				t.Fatalf("canceled snapshot applied %d times", applied)
			}
			assertRecoveryIdle(t, r)
		})
	}
}

func TestRecoveryEnsureRejectedRevisionCanRetry(t *testing.T) {
	cluster := newRecoveryCluster()
	directory := &recoveryDirectory{rejectRemaining: 1}
	r := New(1, cluster, directory, &recoveryConnManager{})
	encoded := encodeRecoveryConn(t, recoveryTestConn())
	var calls atomic.Int32
	cluster.request = func(_ context.Context, node uint64, _ string, body []byte) (*proto.Response, error) {
		calls.Add(1)
		var req request
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, err
		}
		data, _ := json.Marshal(result{request: req, Owner: node, Conns: [][]byte{encoded}})
		if err := r.receive(data); err != nil {
			return nil, err
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	if err := r.Ensure(recoveryContext(t), "alice"); err != nil {
		t.Fatal(err)
	}
	if begun, applied := directory.counts(); begun != 2 || applied != 2 || calls.Load() != 2 {
		t.Fatalf("revision retry: begin=%d apply=%d requests=%d", begun, applied, calls.Load())
	}
	if len(directory.applied) != 1 || !directory.applied[0].SameSession(recoveryTestConn()) {
		t.Fatal("successful retry changed the recovered session")
	}
	assertRecoveryIdle(t, r)
}

func TestRecoveryEnsureDoesNotApplyAfterSnapshotCancellation(t *testing.T) {
	cluster := newRecoveryCluster()
	cluster.nodes = cluster.nodes[:1]
	conn := recoveryTestConn()
	conn.NodeId = 1
	directory := &recoveryDirectory{local: []*eventbus.Conn{conn}}
	socket := &recoverySocket{context: conn}
	ctx, cancel := context.WithCancel(recoveryContext(t))
	defer cancel()
	manager := &recoveryConnManager{get: func(int64) wknet.Conn {
		// 查询已进入快照阶段后取消，不能将随后完成的快照写入目录。
		cancel()
		return socket
	}}
	r := New(1, cluster, directory, manager)
	if err := r.Ensure(ctx, "alice"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
	if _, applied := directory.counts(); applied != 0 {
		t.Fatalf("snapshot applied after cancellation: %d", applied)
	}
	assertRecoveryIdle(t, r)
}

func TestRecoveryQueryFailureCleansPending(t *testing.T) {
	tests := []struct {
		name string
		resp *proto.Response
		err  error
	}{
		{name: "nil response"},
		{name: "rejected response", resp: &proto.Response{Status: proto.StatusError}},
		{name: "transport error", err: errors.New("owner unreachable")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := newRecoveryCluster()
			cluster.request = func(context.Context, uint64, string, []byte) (*proto.Response, error) {
				return test.resp, test.err
			}
			r := New(1, cluster, &recoveryDirectory{}, &recoveryConnManager{})
			if _, err := r.query(recoveryContext(t), 2, "alice", 7); err == nil {
				t.Fatal("owner failure accepted")
			}
			assertRecoveryIdle(t, r)
		})
	}
}

func TestRecoveryRespondOnlySendsKeysToConfiguredLeader(t *testing.T) {
	cluster := newRecoveryCluster()
	conn := recoveryTestConn()
	r := New(2, cluster, &recoveryDirectory{local: []*eventbus.Conn{conn}}, &recoveryConnManager{
		conn: &recoverySocket{context: conn},
	})
	var calls int
	var callback result
	cluster.request = func(_ context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
		calls++
		if node != cluster.leader || path != resultPath {
			return nil, fmt.Errorf("keys sent to unexpected destination %d %s", node, path)
		}
		if err := json.Unmarshal(body, &callback); err != nil {
			return nil, err
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	req := request{UID: conn.Uid, Leader: 1, Version: 7, Nonce: strings.Repeat("a", 64)}
	// respond 的返回值只有错误，密钥结果仅交给配置地址的出站回调。
	if err := r.respond(recoveryContext(t), recoveryJSON(t, req)); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || callback.request != req || callback.Owner != 2 || len(callback.Conns) != 1 {
		t.Fatalf("unexpected callback: calls=%d owner=%d conns=%d", calls, callback.Owner, len(callback.Conns))
	}
	decoded := &eventbus.Conn{}
	if err := decoded.Decode(callback.Conns[0]); err != nil || !decoded.SameSession(conn) {
		t.Fatalf("configured leader did not receive the original session: %v", err)
	}
	// 任意入站请求即使自选 nonce，也不能指定非当前领导节点接收密钥。
	req.Leader = 2
	if err := r.respond(recoveryContext(t), recoveryJSON(t, req)); err == nil || calls != 1 {
		t.Fatal("forged leader received a callback")
	}
}

func TestRecoveryRespondRejectsInvalidRequest(t *testing.T) {
	base := request{UID: "alice", Leader: 1, Version: 7, Nonce: strings.Repeat("b", 64)}
	tests := []struct {
		name string
		edit func(*request)
	}{
		{name: "empty uid", edit: func(r *request) { r.UID = "" }},
		{name: "long uid", edit: func(r *request) { r.UID = strings.Repeat("u", 4097) }},
		{name: "wrong version", edit: func(r *request) { r.Version++ }},
		{name: "short nonce", edit: func(r *request) { r.Nonce = "b" }},
		{name: "non hex nonce", edit: func(r *request) { r.Nonce = strings.Repeat("z", 64) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := newRecoveryCluster()
			var requests int
			cluster.request = func(context.Context, uint64, string, []byte) (*proto.Response, error) {
				requests++
				return &proto.Response{Status: proto.StatusOK}, nil
			}
			r := New(2, cluster, &recoveryDirectory{}, &recoveryConnManager{})
			req := base
			test.edit(&req)
			if err := r.respond(recoveryContext(t), recoveryJSON(t, req)); err == nil || requests != 0 {
				t.Fatalf("invalid request accepted: err=%v callbacks=%d", err, requests)
			}
		})
	}
}

func TestRecoveryReceiveRejectsUnsolicitedOrInvalidSession(t *testing.T) {
	tests := []struct {
		name string
		edit func(*Recovery, *recoveryCluster, *pending, *result, *eventbus.Conn)
	}{
		{name: "unsolicited nonce", edit: func(r *Recovery, _ *recoveryCluster, p *pending, _ *result, _ *eventbus.Conn) {
			delete(r.pending, p.Nonce)
		}},
		{name: "expired nonce", edit: func(_ *Recovery, _ *recoveryCluster, p *pending, _ *result, _ *eventbus.Conn) {
			p.deadline = time.Now().Add(-time.Second)
		}},
		{name: "wrong owner", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, r *result, _ *eventbus.Conn) { r.Owner = 1 }},
		{name: "wrong uid", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, r *result, _ *eventbus.Conn) { r.UID = "bob" }},
		{name: "wrong version", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, r *result, _ *eventbus.Conn) { r.Version++ }},
		{name: "wrong leader", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, r *result, _ *eventbus.Conn) { r.Leader = 2 }},
		{name: "leader changed", edit: func(_ *Recovery, c *recoveryCluster, _ *pending, _ *result, _ *eventbus.Conn) { c.leader = 2 }},
		{name: "configuration changed", edit: func(_ *Recovery, c *recoveryCluster, _ *pending, _ *result, _ *eventbus.Conn) { c.version++ }},
		{name: "owner offline", edit: func(_ *Recovery, c *recoveryCluster, _ *pending, _ *result, _ *eventbus.Conn) {
			c.nodes[1].Online = false
		}},
		{name: "connection uid", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, _ *result, c *eventbus.Conn) { c.Uid = "bob" }},
		{name: "connection owner", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, _ *result, c *eventbus.Conn) { c.NodeId = 1 }},
		{name: "connection id", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, _ *result, c *eventbus.Conn) { c.ConnId = 0 }},
		{name: "unauthenticated", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, _ *result, c *eventbus.Conn) { c.Auth = false }},
		{name: "internal connection", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, _ *result, c *eventbus.Conn) { c.Internal = true }},
		{name: "missing session", edit: func(_ *Recovery, _ *recoveryCluster, _ *pending, _ *result, c *eventbus.Conn) { c.SessionId = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := newRecoveryCluster()
			r := New(1, cluster, &recoveryDirectory{}, &recoveryConnManager{})
			p := &pending{
				request: request{UID: "alice", Leader: 1, Version: 7, Nonce: strings.Repeat("c", 64)},
				owner:   2, deadline: time.Now().Add(time.Minute), result: make(chan []*eventbus.Conn, 1),
			}
			r.pending[p.Nonce] = p
			res := result{request: p.request, Owner: p.owner}
			conn := recoveryTestConn()
			test.edit(r, cluster, p, &res, conn)
			res.Conns = [][]byte{encodeRecoveryConn(t, conn)}
			if err := r.receive(recoveryJSON(t, res)); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
			if len(p.result) != 0 {
				t.Fatal("invalid snapshot reached the apply channel")
			}
		})
	}
}

func TestRecoveryReceiveAcceptsOnceAndRefreshesActivity(t *testing.T) {
	r := New(1, newRecoveryCluster(), &recoveryDirectory{}, &recoveryConnManager{})
	p := &pending{
		request: request{UID: "alice", Leader: 1, Version: 7, Nonce: strings.Repeat("d", 64)},
		owner:   2, deadline: time.Now().Add(time.Minute), result: make(chan []*eventbus.Conn, 1),
	}
	r.pending[p.Nonce] = p
	conn := recoveryTestConn()
	body := recoveryJSON(t, result{request: p.request, Owner: 2, Conns: [][]byte{encodeRecoveryConn(t, conn)}})
	// 并发重放只能有一次成功，其他回调不能阻塞或产生第二份结果。
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r.receive(body) == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 1 || len(p.result) != 1 {
		t.Fatalf("replay accepted=%d results=%d", accepted.Load(), len(p.result))
	}
	conns := <-p.result
	if len(conns) != 1 || !conns[0].SameSession(conn) || conns[0].LastActive <= conn.LastActive {
		t.Fatal("recovery did not preserve session and initialize activity")
	}
	if err := r.receive(body); err == nil {
		t.Fatal("consumed nonce was accepted again")
	}
	assertRecoveryIdle(t, r)
}

func TestRecoveryReceiveRejectsMalformedOrDuplicateConnections(t *testing.T) {
	for _, name := range []string{"malformed", "duplicate", "oversized", "invalid json"} {
		t.Run(name, func(t *testing.T) {
			r := New(1, newRecoveryCluster(), &recoveryDirectory{}, &recoveryConnManager{})
			p := &pending{
				request: request{UID: "alice", Leader: 1, Version: 7, Nonce: strings.Repeat("e", 64)},
				owner:   2, deadline: time.Now().Add(time.Minute), result: make(chan []*eventbus.Conn, 1),
			}
			r.pending[p.Nonce] = p
			encoded := encodeRecoveryConn(t, recoveryTestConn())
			res := result{request: p.request, Owner: 2, Conns: [][]byte{encoded}}
			if name == "malformed" {
				res.Conns[0] = []byte{1}
			} else if name == "duplicate" {
				res.Conns = append(res.Conns, encoded)
			}
			body := recoveryJSON(t, res)
			if name == "oversized" {
				body = bytes.Repeat([]byte(" "), maxBodySize+1)
			} else if name == "invalid json" {
				body = []byte("{")
			}
			if err := r.receive(body); err == nil || len(p.result) != 0 {
				t.Fatalf("invalid body accepted: err=%v results=%d", err, len(p.result))
			}
		})
	}
}

func TestRecoveryLocalSnapshotRequiresLiveAuthenticatedSession(t *testing.T) {
	tests := []struct {
		name    string
		edit    func(*eventbus.Conn, *eventbus.Conn, *recoverySocket, *recoveryConnManager)
		want    int
		wantErr bool
	}{
		{name: "matching session", want: 1},
		{name: "missing socket", edit: func(_, _ *eventbus.Conn, _ *recoverySocket, m *recoveryConnManager) { m.conn = nil }},
		{name: "closed socket", edit: func(_, _ *eventbus.Conn, s *recoverySocket, _ *recoveryConnManager) { s.closed = true }},
		{name: "untyped context", edit: func(_, _ *eventbus.Conn, s *recoverySocket, _ *recoveryConnManager) { s.context = "untrusted" }},
		{name: "unauthenticated", edit: func(_, c *eventbus.Conn, _ *recoverySocket, _ *recoveryConnManager) { c.Auth = false }},
		{name: "internal", edit: func(_, c *eventbus.Conn, _ *recoverySocket, _ *recoveryConnManager) { c.Internal = true }},
		{name: "missing session", edit: func(_, c *eventbus.Conn, _ *recoverySocket, _ *recoveryConnManager) { c.SessionId = "" }, wantErr: true},
		{name: "reused connection id", edit: func(_, c *eventbus.Conn, _ *recoverySocket, _ *recoveryConnManager) {
			c.SessionId = "replacement-session"
		}},
		{name: "wrong uid", edit: func(_, c *eventbus.Conn, _ *recoverySocket, _ *recoveryConnManager) { c.Uid = "bob" }},
		{name: "wrong owner", edit: func(_, c *eventbus.Conn, _ *recoverySocket, _ *recoveryConnManager) { c.NodeId = 1 }},
		{name: "invalid candidate id", edit: func(c, _ *eventbus.Conn, _ *recoverySocket, _ *recoveryConnManager) { c.ConnId = 0 }},
		{name: "foreign candidate", edit: func(c, _ *eventbus.Conn, _ *recoverySocket, _ *recoveryConnManager) { c.NodeId = 1 }},
		{name: "changed key", edit: func(_, c *eventbus.Conn, _ *recoverySocket, _ *recoveryConnManager) {
			c.AesKey = []byte("replacement-key!")
		}},
		{name: "closed during snapshot", edit: func(_, _ *eventbus.Conn, s *recoverySocket, _ *recoveryConnManager) {
			calls := 0
			s.isClosed = func() bool { calls++; return calls > 1 }
		}},
		{name: "replaced during snapshot", edit: func(_, _ *eventbus.Conn, s *recoverySocket, m *recoveryConnManager) {
			calls := 0
			m.get = func(int64) wknet.Conn {
				calls++
				if calls == 1 {
					return s
				}
				return &recoverySocket{context: recoveryTestConn()}
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate, real := recoveryTestConn(), recoveryTestConn()
			socket := &recoverySocket{context: real}
			manager := &recoveryConnManager{conn: socket}
			if test.edit != nil {
				test.edit(candidate, real, socket, manager)
			}
			r := New(2, newRecoveryCluster(), &recoveryDirectory{local: []*eventbus.Conn{candidate}}, manager)
			conns, err := r.localSnapshot("alice")
			if (err != nil) != test.wantErr || len(conns) != test.want {
				t.Fatalf("snapshot len=%d err=%v, want len=%d error=%v", len(conns), err, test.want, test.wantErr)
			}
			if test.want == 0 {
				return
			}
			if conns[0] == real || !conns[0].SameSession(real) || conns[0].LastActive <= real.LastActive {
				t.Fatal("snapshot is not an independent authenticated session")
			}
			conns[0].AesKey[0] ^= 1
			if bytes.Equal(conns[0].AesKey, real.AesKey) {
				t.Fatal("snapshot shares encryption storage with live connection")
			}
		})
	}
}
