package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func sessionRevokeRequest(t *testing.T, f *sessionRaceFixture, path string, body map[string]interface{}) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	f.router.GetGinRoute().ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(data))))
	return w
}

type cancelAfterSourceQuery struct {
	*sessionRaceCluster
	cancel context.CancelFunc
}

func (c *cancelAfterSourceQuery) RequestWithContext(ctx context.Context, node uint64, path string, data []byte) (*proto.Response, error) {
	resp, err := c.sessionRaceCluster.RequestWithContext(ctx, node, path, data)
	if path == "/wk/ingress/userConns" {
		c.cancel()
	}
	return resp, err
}

func TestConditionalRevokeStopsAfterSourceQueryCancellation(t *testing.T) {
	f := newSessionRaceFixtureConfig(t, 1, false)
	remote := sessionRaceConn(1)
	remote.NodeId = 2
	remote.AuthTokenVersion, remote.AuthTokenVersionKnown = 41, true
	f.cluster.peers = map[uint64][]*eventbus.Conn{2: {remote}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Cluster = &cancelAfterSourceQuery{sessionRaceCluster: f.cluster, cancel: cancel}
	err := newUser(nil).revokeTokenConnections(remote.Uid, remote.DeviceFlag, true, eventbus.TokenFingerprint("revoke"), &deviceSessionTarget{Version: 41, Hash: eventbus.TokenFingerprint("old-token"), ctx: ctx})
	if !errors.Is(err, context.Canceled) || len(f.cluster.revoked) != 0 {
		t.Fatal("查询取消后仍发起了关闭 RPC")
	}
}

type cancelBeforeLocalClose struct {
	*sessionRaceCluster
	cancel context.CancelFunc
}

func (c *cancelBeforeLocalClose) SlotLeaderId(slot uint32) uint64 {
	c.cancel()
	return c.sessionRaceCluster.SlotLeaderId(slot)
}

func TestConditionalRevokeStopsBeforeLocalCloseWhenCanceled(t *testing.T) {
	f := newSessionRaceFixtureConfig(t, 1, false)
	conn := sessionRaceConn(1)
	conn.AuthTokenVersion, conn.AuthTokenVersionKnown = 41, true
	f.users.UpdateConn(conn)
	f.real.SetContext(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Cluster = &cancelBeforeLocalClose{sessionRaceCluster: f.cluster, cancel: cancel}
	err := newUser(nil).revokeTokenConnections(conn.Uid, conn.DeviceFlag, true, eventbus.TokenFingerprint("revoke"), &deviceSessionTarget{Version: 41, Hash: conn.AuthTokenHash, ctx: ctx})
	if !errors.Is(err, context.Canceled) || f.real.closed.Load() || !conn.Revoked.Load() {
		t.Fatal("关闭循环前取消后不应继续执行本地关闭")
	}
}

func TestDeviceSessionSnapshotDoesNotExposeToken(t *testing.T) {
	f := newSessionRaceFixture(t)
	w := sessionRevokeRequest(t, f, "/user/token/session", map[string]interface{}{"uid": "session-race-user", "device_flag": 0})
	if w.Code != http.StatusOK {
		t.Fatalf("查询失败: %d %s", w.Code, w.Body.String())
	}
	var body struct {
		Exists  bool   `json:"exists"`
		Version uint64 `json:"token_version"`
		Hash    string `json:"token_hash"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Exists || body.Version != 41 || body.Hash != eventbus.TokenFingerprint("old-token") || strings.Contains(w.Body.String(), "old-token") {
		t.Fatalf("快照必须只返回目标代际和指纹: %s", w.Body.String())
	}
}

func TestDeviceSessionSnapshotKeepsEmptyTokenRevocationProof(t *testing.T) {
	f := newSessionRaceFixture(t)
	f.db.mu.Lock()
	f.db.device.Token = ""
	f.db.mu.Unlock()
	w := sessionRevokeRequest(t, f, "/user/token/session", map[string]interface{}{"uid": "session-race-user", "device_flag": 0})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	var proof struct {
		Exists  bool   `json:"exists"`
		Hash    string `json:"token_hash"`
		Version uint64 `json:"token_version"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &proof); err != nil {
		t.Fatal(err)
	}
	if !proof.Exists || proof.Hash != eventbus.TokenFingerprint("") || proof.Version != 41 {
		t.Fatal("已有空 Token 记录仍须保留代际，以便补完旧连接关闭")
	}
}

func TestConditionalDeviceRevokeIsIdempotentAndKeepsOtherFlags(t *testing.T) {
	f := newSessionRaceFixture(t)
	f.db.mu.Lock()
	f.db.device.DeviceFlag = uint64(wkproto.WEB)
	f.db.mu.Unlock()
	web, app := sessionRaceConn(100), sessionRaceConn(101)
	app.ConnId = 100
	web.DeviceFlag, app.DeviceFlag = wkproto.WEB, wkproto.APP
	web.AuthTokenVersion, web.AuthTokenVersionKnown = 41, true
	f.users.UpdateConn(web)
	f.users.UpdateConn(app)
	f.real.SetContext(web)
	request := map[string]interface{}{"uid": "session-race-user", "device_flag": 1, "token": "revocation-token", "token_version": 42,
		"expected_token_version": 41, "expected_token_hash": eventbus.TokenFingerprint("old-token")}
	for i := 0; i < 3; i++ {
		w := sessionRevokeRequest(t, f, "/user/token/revoke", request)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"result":"revoked"`) {
			t.Fatalf("同一任务重试失败: %d %s", w.Code, w.Body.String())
		}
	}
	if !web.Revoked.Load() || app.Revoked.Load() || f.users.ConnById(app.Uid, app.NodeId, app.ConnId) == nil {
		t.Fatal("Web 撤销必须精确保留 APP")
	}
}

func TestConditionalWebRevokeKeepsAPPOnline(t *testing.T) {
	f := newSessionRaceFixture(t)
	f.db.mu.Lock()
	f.db.device.DeviceFlag = uint64(wkproto.WEB)
	f.db.mu.Unlock()
	web, app := sessionRaceConn(100), sessionRaceConn(101)
	web.NodeId, web.ConnId, web.DeviceFlag = 2, 200, wkproto.WEB
	web.AuthTokenVersion, web.AuthTokenVersionKnown = 41, true
	app.DeviceFlag = wkproto.APP
	f.cluster.peers = map[uint64][]*eventbus.Conn{2: {web}}
	f.users.UpdateConn(web)
	f.users.UpdateConn(app)
	f.real.SetContext(app)
	w := sessionRevokeRequest(t, f, "/user/token/revoke", map[string]interface{}{"uid": "session-race-user", "device_flag": 1, "token": "revoke", "token_version": 42, "expected_token_version": 41, "expected_token_hash": eventbus.TokenFingerprint("old-token")})
	if w.Code != http.StatusOK || f.real.closed.Load() || app.Revoked.Load() {
		t.Fatal("桌面退出影响了 APP 在线连接")
	}
	f.handler.OnEvent(&eventbus.UserContext{Uid: app.Uid, EventType: eventbus.EventOnSend, Events: []*eventbus.Event{{Type: eventbus.EventOnSend, Conn: app, Frame: &wkproto.PingPacket{}}}})
	f.users.mu.Lock()
	defer f.users.mu.Unlock()
	for _, event := range f.users.events {
		if _, ok := event.Frame.(*wkproto.PongPacket); ok && event.Conn.SameSession(app) {
			return
		}
	}
	t.Fatal("桌面退出后 APP 心跳仍应正常进入消息处理")
}

func TestConditionalDeviceRevokeCannotChaseNewSession(t *testing.T) {
	for _, sameToken := range []bool{false, true} {
		t.Run(map[bool]string{false: "new-token", true: "same-token-new-version"}[sameToken], func(t *testing.T) {
			f := newSessionRaceFixture(t)
			newToken := "new-token"
			if sameToken {
				newToken = "old-token"
			}
			if w := f.update(newToken, 43); w.Code != http.StatusOK {
				t.Fatal(w.Body.String())
			}
			conn := sessionRaceConn(200)
			conn.AuthTokenHash = eventbus.TokenFingerprint(newToken)
			f.users.UpdateConn(conn)
			f.real.SetContext(conn)
			// 即使旧任务已错误升版到 500，也不得绕过对原目标的比较。
			w := sessionRevokeRequest(t, f, "/user/token/revoke", map[string]interface{}{"uid": "session-race-user", "device_flag": 0, "token": "revoke", "token_version": 500,
				"expected_token_version": 41, "expected_token_hash": eventbus.TokenFingerprint("old-token")})
			if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"result":"superseded"`) {
				t.Fatalf("应无副作用淘汰: %d %s", w.Code, w.Body.String())
			}
			f.db.mu.Lock()
			device := f.db.device
			f.db.mu.Unlock()
			if device.Token != newToken || device.TokenVersion != 43 || conn.Revoked.Load() || f.real.closed.Load() {
				t.Fatal("条件撤销影响了新会话")
			}
		})
	}
}

func TestConditionalDeviceRevokeValidatesProofBeforeStore(t *testing.T) {
	f := newSessionRaceFixture(t)
	for _, body := range []map[string]interface{}{
		{"uid": "u", "device_flag": 0, "token": "revoke", "token_version": 42},
		{"uid": "u", "device_flag": 0, "token": "revoke", "token_version": 42, "expected_token_hash": eventbus.TokenFingerprint("t")},
		{"uid": "u", "device_flag": 0, "token": "revoke", "token_version": 42, "expected_token_version": 41, "expected_token_hash": "bad"},
		{"uid": "u", "device_flag": 9, "token": "revoke", "token_version": 42, "expected_token_version": 41, "expected_token_hash": eventbus.TokenFingerprint("t")},
	} {
		w := sessionRevokeRequest(t, f, "/user/token/revoke", body)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("缺失或无效证据不能触发撤销: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestConditionalDeviceRevokeRetriesFailedRemoteClose(t *testing.T) {
	f := newSessionRaceFixture(t)
	remote := sessionRaceConn(300)
	remote.AuthTokenVersion, remote.AuthTokenVersionKnown = 41, true
	remote.NodeId = 2
	f.cluster.peers = map[uint64][]*eventbus.Conn{2: {remote}}
	f.cluster.requestErr = errors.New("remote unavailable")
	body := map[string]interface{}{"uid": "session-race-user", "device_flag": 0, "token": "revocation-token", "token_version": 42, "expected_token_version": 41, "expected_token_hash": eventbus.TokenFingerprint("old-token")}
	w := sessionRevokeRequest(t, f, "/user/token/revoke", body)
	if w.Code == http.StatusOK {
		t.Fatal("关闭未确认前不能返回撤销完成")
	}
	f.cluster.requestErr = nil
	w = sessionRevokeRequest(t, f, "/user/token/revoke", body)
	if w.Code != http.StatusOK || !f.cluster.remoteClosed.Load() {
		t.Fatalf("相同任务必须完成远端关闭: %d %s", w.Code, w.Body.String())
	}
}

func TestConditionalCloseOnlyTouchesOriginalCredentialGeneration(t *testing.T) {
	f := newSessionRaceFixture(t)
	old, current, sameTokenNew := sessionRaceConn(1), sessionRaceConn(2), sessionRaceConn(3)
	for _, conn := range []*eventbus.Conn{old, current, sameTokenNew} {
		conn.NodeId = 2
	}
	old.AuthTokenVersion = 41
	old.AuthTokenVersionKnown = true
	current.AuthTokenHash = eventbus.TokenFingerprint("new-token")
	current.AuthTokenVersion = 43
	current.AuthTokenVersionKnown = true
	sameTokenNew.AuthTokenVersion = 43
	sameTokenNew.AuthTokenVersionKnown = true
	// 模拟旧 leader 版本复核后，源节点已经有更高代际连接；关闭不能只排除撤销 Token。
	f.cluster.afterApply = func() { f.cluster.peers = map[uint64][]*eventbus.Conn{2: {old, current, sameTokenNew}} }
	w := sessionRevokeRequest(t, f, "/user/token/revoke", map[string]interface{}{"uid": "session-race-user", "device_flag": 0, "token": "revoke", "token_version": 42, "expected_token_version": 41, "expected_token_hash": eventbus.TokenFingerprint("old-token")})
	if w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if len(f.cluster.revoked) != 1 || f.cluster.revoked[0].SessionId != old.SessionId {
		t.Fatal("新 Token 或同 Token 更高代际连接被误踢")
	}
}

func TestConditionalCloseDoesNotGuessMissingConnectionGeneration(t *testing.T) {
	f := newSessionRaceFixture(t)
	unknown := sessionRaceConn(1)
	unknown.NodeId = 2
	f.cluster.peers = map[uint64][]*eventbus.Conn{2: {unknown}}
	w := sessionRevokeRequest(t, f, "/user/token/revoke", map[string]interface{}{"uid": "session-race-user", "device_flag": 0, "token": "revoke", "token_version": 42, "expected_token_version": 41, "expected_token_hash": eventbus.TokenFingerprint("old-token")})
	if w.Code == http.StatusOK || len(f.cluster.revoked) != 0 || unknown.Revoked.Load() {
		t.Fatal("旧事件缺少代际时不能猜测并误踢，也不能虚报完成")
	}
}

func TestSnapshotRejectsUnknownSourceBeforeCredentialChanges(t *testing.T) {
	f := newSessionRaceFixture(t)
	unknown := sessionRaceConn(1)
	f.real.SetContext(unknown)
	w := sessionRevokeRequest(t, f, "/user/token/session", map[string]interface{}{"uid": "session-race-user", "device_flag": 0})
	if w.Code == http.StatusOK {
		t.Fatal("不能为没有认证代际的旧源连接生成退出证明")
	}
	f.db.mu.Lock()
	defer f.db.mu.Unlock()
	if f.db.device.Token != "old-token" || unknown.Revoked.Load() || f.real.closed.Load() {
		t.Fatal("只读快照失败不应改变凭据或断开连接")
	}
}

func TestConditionalCloseUsesVerifiedSourceInsteadOfLegacyDirectory(t *testing.T) {
	f := newSessionRaceFixture(t)
	directory := sessionRaceConn(1)
	directory.NodeId = 2
	data, err := directory.Encode()
	if err != nil {
		t.Fatal(err)
	}
	source := &eventbus.Conn{}
	if err := source.Decode(data); err != nil {
		t.Fatal(err)
	}
	source.AuthTokenVersion, source.AuthTokenVersionKnown = 41, true
	f.users.UpdateConn(directory)
	f.cluster.peers = map[uint64][]*eventbus.Conn{2: {source}}
	w := sessionRevokeRequest(t, f, "/user/token/revoke", map[string]interface{}{"uid": "session-race-user", "device_flag": 0, "token": "revoke", "token_version": 42, "expected_token_version": 41, "expected_token_hash": eventbus.TokenFingerprint("old-token")})
	if w.Code != http.StatusOK || len(f.cluster.revoked) != 1 || !directory.Revoked.Load() {
		t.Fatalf("同一物理连接的源证明应可补齐旧目录: %d %s", w.Code, w.Body.String())
	}
}

// 仅给跨仓库本地协议测试启动，正常测试和生产不会打开控制路由。
func TestDeviceSessionContractProcess(t *testing.T) {
	if os.Getenv("WK_TEST_TARGETED_SESSION_CONTRACT") != "1" {
		t.Skip("仅供 API/IM 本地配套测试")
	}
	f := newSessionRaceFixture(t)
	stop := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("/user/", f.router.GetGinRoute())
	mux.HandleFunc("/test/stop", func(w http.ResponseWriter, r *http.Request) { close(stop) })
	server := httptest.NewServer(mux)
	defer server.Close()
	fmt.Printf("DEVICE_SESSION_CONTRACT=%s\n", server.URL)
	select {
	case <-stop:
	case <-time.After(90 * time.Second):
		t.Fatal("本地配套测试未完成")
	}
}
