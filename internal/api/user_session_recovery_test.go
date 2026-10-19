package api

import (
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// 将原先断言坏状态的 proof 改为正确行为：落库后的远端查询失败不能被恢复绕过。
func TestTokenRecoveryKeepsRevocationAfterSourceFailure(t *testing.T) {
	f := newSessionRaceFixture(t)
	old := sessionRaceConn(100)
	old.NodeId = 2
	f.users.UpdateConn(old)
	f.cluster.peers = map[uint64][]*eventbus.Conn{2: {old}}
	f.cluster.requestErr = errors.New("模拟远端连接查询失败")
	if response := f.update("new-token", 42); response.Code != http.StatusBadRequest {
		t.Fatalf("预期模拟失败，实际 %d", response.Code)
	}
	if !old.Revoked.Load() {
		t.Fatal("必须先撤销旧目录中的会话")
	}
	source := recoverySessionSnapshot(t, old)
	deadline := time.Now().Add(time.Second)
	for {
		apply, needed := f.pool.BeginConnRecovery(old.Uid, 7)
		if needed && apply != nil && apply([]*eventbus.Conn{source}) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Token 任务退出后应能完成安全恢复")
		}
		time.Sleep(time.Millisecond)
	}
	current := f.users.ConnById(old.Uid, old.NodeId, old.ConnId)
	if current == nil || !current.Revoked.Load() {
		t.Fatal("恢复清除了当前代际的撤销依据")
	}
	if len(f.users.AuthedConnsByUid(old.Uid)) != 0 {
		t.Fatal("源 RPC 失败尚未关闭的撤销会话仍被选择为在线接收者")
	}
	assertRecoverySessionFrames(t, f, source, false)
}

func TestTokenRecoveryKeepsSameTokenRevocationButAllowsNewGeneration(t *testing.T) {
	f := newSessionRaceFixtureConfig(t, 1, false)
	old := sessionRaceConn(100)
	old.NodeId = 2
	old.Revoked.Store(true)
	f.users.UpdateConn(old)
	source := recoverySessionSnapshot(t, old)
	apply, _ := f.pool.BeginConnRecovery(old.Uid, 7)
	if !apply([]*eventbus.Conn{source}) {
		t.Fatal("同 Token 的源快照应能完成恢复")
	}
	current := f.users.ConnById(old.Uid, old.NodeId, old.ConnId)
	if current == nil || !current.Revoked.Load() {
		t.Fatal("凭据仍相同也不得复活已撤销的会话代际")
	}
	assertRecoverySessionFrames(t, f, source, false)
	// 同一连接号的新会话不继承旧代际的撤销，旧客户端不增加任何协议字段。
	fresh := sessionRaceConn(100)
	fresh.NodeId = old.NodeId
	apply, _ = f.pool.BeginConnRecovery(old.Uid, 8)
	if !apply([]*eventbus.Conn{recoverySessionSnapshot(t, fresh)}) {
		t.Fatal("同 Token 的新代际应能恢复")
	}
	current = f.users.ConnById(fresh.Uid, fresh.NodeId, fresh.ConnId)
	if !current.SameSession(fresh) || current.Revoked.Load() {
		t.Fatal("旧代际的撤销误伤正常重连")
	}
	assertRecoverySessionFrames(t, f, fresh, true)
}

func TestTokenRecoveryNewLeaderRejectsInvalidCredentials(t *testing.T) {
	for _, mode := range []string{"stale-token", "empty-token", "missing-fingerprint", "device-error"} {
		t.Run(mode, func(t *testing.T) {
			f := newSessionRaceFixtureConfig(t, 1, false)
			db := installRecoverySessionDB(f)
			stale, healthy := sessionRaceConn(100), sessionRaceConn(200)
			stale.NodeId, healthy.NodeId = 2, 3
			stale.AuthTokenHash = eventbus.TokenFingerprint("stale-token")
			switch mode {
			case "empty-token":
				f.db.device.Token = ""
			case "missing-fingerprint":
				stale.AuthTokenHash = ""
			case "device-error":
				db.read = func(wkdb.Device) (wkdb.Device, error) {
					return wkdb.Device{}, errors.New("模拟当前凭据读取失败")
				}
			}
			apply, _ := f.pool.BeginConnRecovery(stale.Uid, 7)
			applied := apply([]*eventbus.Conn{recoverySessionSnapshot(t, stale), recoverySessionSnapshot(t, healthy)})
			if got := f.users.ConnById(stale.Uid, stale.NodeId, stale.ConnId); got != nil {
				t.Fatal("无本地撤销记忆的新 leader 导入了无效凭据的源快照")
			}
			if mode == "device-error" {
				if applied || f.users.ConnById(healthy.Uid, healthy.NodeId, healthy.ConnId) != nil {
					t.Fatal("凭据读取失败不得部分应用或确认恢复")
				}
				return
			}
			if !applied {
				t.Fatal("过时源会话不应阻止本次安全恢复完成")
			}
			if mode != "empty-token" {
				if got := f.users.ConnById(healthy.Uid, healthy.NodeId, healthy.ConnId); !got.SameSession(healthy) || got.Revoked.Load() {
					t.Fatal("同一设备类型的合法会话被过时快照误伤")
				}
				if db.reads.Load() != 1 {
					t.Fatal("恢复应按设备类型读取一次当前凭据")
				}
			}
			assertRecoverySessionFrames(t, f, stale, false)
		})
	}
}

func TestTokenRecoveryDoesNotQueryCredentialsPerMessage(t *testing.T) {
	f := newSessionRaceFixtureConfig(t, 1, false)
	db := installRecoverySessionDB(f)
	conn := sessionRaceConn(100)
	conn.NodeId = 2
	apply, _ := f.pool.BeginConnRecovery(conn.Uid, 7)
	if !apply([]*eventbus.Conn{recoverySessionSnapshot(t, conn)}) {
		t.Fatal("新 leader 应能恢复当前 Token 的会话")
	}
	if db.reads.Load() != 1 {
		t.Fatalf("恢复未核验当前凭据，查询次数 %d", db.reads.Load())
	}
	for i := 0; i < 3; i++ {
		assertRecoverySessionFrames(t, f, conn, true)
	}
	if db.reads.Load() != 1 {
		t.Fatal("正常 SEND、RECVACK、PING 不得逐消息查询凭据")
	}
}

func TestTokenRecoveryRejectsStaleDeviceLevel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current wkproto.DeviceLevel
		stale   wkproto.DeviceLevel
	}{
		{name: "升主拒旧从", current: wkproto.DeviceLevelMaster, stale: wkproto.DeviceLevelSlave},
		{name: "降从拒旧主", current: wkproto.DeviceLevelSlave, stale: wkproto.DeviceLevelMaster},
	} {
		for _, registered := range []bool{false, true} {
			name := tc.name + "/new-leader"
			if registered {
				name = tc.name + "/known-session"
			}
			t.Run(name, func(t *testing.T) {
				f := newSessionRaceFixtureConfig(t, 1, false)
				f.db.device.DeviceLevel = uint8(tc.current)
				old, healthy := sessionRaceConn(100), sessionRaceConn(200)
				old.NodeId, old.DeviceLevel = 2, tc.stale
				healthy.NodeId, healthy.DeviceLevel = 3, tc.current
				// 保留真实源快照，避免补恢复因源缺失而假通过。
				f.cluster.peers = map[uint64][]*eventbus.Conn{2: {old}, 3: {healthy}}
				if registered {
					f.users.UpdateConn(old)
				}
				apply, _ := f.pool.BeginConnRecovery(old.Uid, 7)
				if !apply([]*eventbus.Conn{recoverySessionSnapshot(t, old), recoverySessionSnapshot(t, healthy)}) {
					t.Fatal("等级过时的源会话不应阻止有效会话恢复")
				}
				got := f.users.ConnById(old.Uid, old.NodeId, old.ConnId)
				if got != nil && !got.Revoked.Load() {
					t.Error("相同 Token 的过时设备等级仍被恢复为可用会话")
				}
				if registered && (got != old || !old.Revoked.Load()) {
					t.Error("同代过时等级必须保留原撤销引用")
				}
				assertRecoverySessionFrames(t, f, recoverySessionSnapshot(t, old), false)
				assertRecoverySessionFrames(t, f, healthy, true)
			})
		}
	}
}

func TestTokenSourceRestoreChecksDeviceLevelWithoutRewriting(t *testing.T) {
	for _, tc := range []struct {
		name     string
		current  wkproto.DeviceLevel
		incoming wkproto.DeviceLevel
		source   wkproto.DeviceLevel
		accepted bool
	}{
		{name: "升主拒旧从", current: wkproto.DeviceLevelMaster, incoming: wkproto.DeviceLevelSlave, source: wkproto.DeviceLevelSlave},
		{name: "降从拒旧主", current: wkproto.DeviceLevelSlave, incoming: wkproto.DeviceLevelMaster, source: wkproto.DeviceLevelMaster},
		{name: "源仍是旧从", current: wkproto.DeviceLevelMaster, incoming: wkproto.DeviceLevelMaster, source: wkproto.DeviceLevelSlave},
		{name: "源仍是旧主", current: wkproto.DeviceLevelSlave, incoming: wkproto.DeviceLevelSlave, source: wkproto.DeviceLevelMaster},
		{name: "当前主等级", current: wkproto.DeviceLevelMaster, incoming: wkproto.DeviceLevelMaster, source: wkproto.DeviceLevelMaster, accepted: true},
		{name: "当前从等级", current: wkproto.DeviceLevelSlave, incoming: wkproto.DeviceLevelSlave, source: wkproto.DeviceLevelSlave, accepted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSessionRaceFixtureConfig(t, 1, false)
			f.db.device.DeviceLevel = uint8(tc.current)
			incoming := sessionRaceConn(100)
			incoming.NodeId, incoming.DeviceLevel = 2, tc.incoming
			source := recoverySessionSnapshot(t, incoming)
			source.DeviceLevel = tc.source
			f.cluster.peers = map[uint64][]*eventbus.Conn{2: {source}}
			assertRecoverySessionFrames(t, f, incoming, tc.accepted)
			if incoming.DeviceLevel != tc.incoming {
				t.Fatal("补恢复不得把旧会话直接改成数据库的新等级")
			}
			got := f.users.ConnById(incoming.Uid, incoming.NodeId, incoming.ConnId)
			if !tc.accepted && got != nil {
				t.Fatal("目录缺失时补恢复重新启用了过时等级会话")
			}
		})
	}
}

func TestTokenRecoveryRejectsCredentialReadOverlappingTokenChange(t *testing.T) {
	f := newSessionRaceFixture(t)
	db := installRecoverySessionDB(f)
	read, resume := make(chan struct{}), make(chan struct{})
	var reads atomic.Int32
	var once sync.Once
	release := func() { once.Do(func() { close(resume) }) }
	defer release()
	db.read = func(device wkdb.Device) (wkdb.Device, error) {
		if reads.Add(1) == 1 {
			close(read)
			<-resume
		}
		return device, nil
	}
	old := sessionRaceConn(100)
	old.NodeId = 2
	apply, _ := f.pool.BeginConnRecovery(old.Uid, 7)
	result := make(chan bool, 1)
	go func() { result <- apply([]*eventbus.Conn{old}) }()
	select {
	case <-read:
	case <-result:
		t.Fatal("恢复没有核验当前凭据，已直接导入旧会话")
	case <-time.After(3 * time.Second):
		t.Fatal("恢复未到达凭据读取检查点")
	}
	if response := f.update("new-token", 42); response.Code != http.StatusOK {
		t.Fatal(response.Body.String())
	}
	release()
	select {
	case applied := <-result:
		if applied || f.users.ConnById(old.Uid, old.NodeId, old.ConnId) != nil {
			t.Fatal("Token 已变更，恢复仍应用了变更前核验的快照")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("恢复与 Token 变更互相阻塞")
	}
}

func recoverySessionSnapshot(t *testing.T, conn *eventbus.Conn) *eventbus.Conn {
	t.Helper()
	wire, err := conn.Encode()
	if err != nil {
		t.Fatal(err)
	}
	copy := &eventbus.Conn{}
	if err := copy.Decode(wire); err != nil {
		t.Fatal(err)
	}
	return copy
}

func assertRecoverySessionFrames(t *testing.T, f *sessionRaceFixture, conn *eventbus.Conn, accepted bool) {
	t.Helper()
	oldPlugin, oldChannel, oldRetry := service.PluginManager, eventbus.Channel, service.RetryManager
	channel, retry := &takeoverChannel{}, &takeoverRetry{}
	service.PluginManager, service.RetryManager = &takeoverPlugin{}, retry
	eventbus.RegisterChannel(channel)
	defer func() {
		service.PluginManager, eventbus.Channel, service.RetryManager = oldPlugin, oldChannel, oldRetry
	}()
	f.users.mu.Lock()
	before := len(f.users.events)
	f.users.mu.Unlock()
	for _, frame := range []wkproto.Frame{
		&wkproto.SendPacket{ChannelID: "receiver", ChannelType: wkproto.ChannelTypePerson, Payload: []byte(`{"type":1,"content":"body"}`)},
		&wkproto.RecvackPacket{MessageID: 12},
		&wkproto.PingPacket{},
	} {
		f.handler.OnEvent(&eventbus.UserContext{Uid: conn.Uid, EventType: eventbus.EventOnSend, AddConn: f.pool.UpdateConn,
			Events: []*eventbus.Event{{Type: eventbus.EventOnSend, Conn: conn, Frame: frame}},
		})
	}
	var pongs int32
	f.users.mu.Lock()
	for _, e := range f.users.events[before:] {
		if _, ok := e.Frame.(*wkproto.PongPacket); ok {
			pongs++
		}
	}
	f.users.mu.Unlock()
	var want int32
	if accepted {
		want = 1
	}
	if channel.sends.Load() != want || retry.acks.Load() != want || pongs != want {
		t.Fatalf("会话准入不符: SEND=%d RECVACK=%d PONG=%d, 期望各 %d", channel.sends.Load(), retry.acks.Load(), pongs, want)
	}
}

type recoverySessionDB struct {
	wkdb.DB
	reads atomic.Int32
	read  func(wkdb.Device) (wkdb.Device, error)
}

func (d *recoverySessionDB) GetDevice(uid string, flag uint64) (wkdb.Device, error) {
	device, err := d.DB.GetDevice(uid, flag)
	d.reads.Add(1)
	if err == nil && d.read != nil {
		return d.read(device)
	}
	return device, err
}

func installRecoverySessionDB(f *sessionRaceFixture) *recoverySessionDB {
	db := &recoverySessionDB{DB: f.db}
	service.Store = store.New(store.NewOptions(store.WithDB(db), store.WithSlot(f.cluster)))
	return db
}
