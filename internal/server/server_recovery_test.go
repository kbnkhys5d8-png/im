package server

import (
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/client"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func migrateProcessUser(t *testing.T, nodes []*clusterProcess, uid string, target uint64) {
	t.Helper()
	slotID := wkutil.GetSlotNum(5, uid)
	state := nodes[0].call(clusterProcessRequest{Op: "user-state", UID: uid})
	if state.NodeID != target {
		processLeader(t, nodes).call(clusterProcessRequest{Op: "migrate", SlotID: slotID, From: state.NodeID, To: target})
	}
	for _, node := range nodes {
		node.waitConfig(func(cfg *types.Config) bool {
			for _, slot := range cfg.Slots {
				if slot.Id == slotID {
					return slot.Leader == target && slot.Status == types.SlotStatus_SlotStatusNormal
				}
			}
			return false
		})
		node.call(clusterProcessRequest{Op: "ready"})
	}
}

// 客户端始终保持原 TCP 和密钥，换主后的第一条消息也必须抵达。
func TestClusterUserConnectionRecovery(t *testing.T) {
	for _, tc := range []struct {
		name        string
		localDevice bool
	}{
		{name: "只有远端接入连接"},
		{name: "新领导已有本地设备仍恢复远端设备", localDevice: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := startProcessCluster(t, 3, 3)
			migrateProcessUser(t, nodes, "u2", nodes[2].config.ID)
			sender := nodes[0].connect("u1")
			receiver := nodes[1].connect("u2")
			messages := receiveProcessMessage(t, receiver)
			var localMessages <-chan string
			if tc.localDevice {
				localMessages = receiveProcessMessage(t, nodes[0].connect("u2"))
			}
			send := func(payload string) {
				t.Helper()
				require.NoError(t, sender.SendMessage(client.NewChannel("u2", 1), []byte(payload)))
				requireProcessMessage(t, messages, payload)
				if localMessages != nil {
					requireProcessMessage(t, localMessages, payload)
				}
			}
			send("before")
			migrateProcessUser(t, nodes, "u2", nodes[0].config.ID)
			send("after")
			want := 1
			if tc.localDevice {
				want = 2
			}
			require.Equal(t, want, nodes[0].call(clusterProcessRequest{Op: "user-state", UID: "u2"}).ConnCount)
			// 再次迁移，验证恢复完成标记不会跨领导任期误用。
			migrateProcessUser(t, nodes, "u2", nodes[2].config.ID)
			send("back")
		})
	}
}

// 旧领导已认证而接入节点尚未收到回执时，新领导先完成恢复也不能漏掉随后登录的设备。
func TestClusterConnackAfterLeaderRecovery(t *testing.T) {
	for _, tc := range []struct {
		name        string
		localDevice bool
		returnToOld bool
	}{
		{name: "新领导已完成空快照恢复"},
		{name: "新领导已恢复另一台在线设备", localDevice: true},
		{name: "换主后返回原领导并恢复空快照", returnToOld: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			nodes := startProcessCluster(t, 3, 3)
			newLeader, owner, oldLeader := nodes[0], nodes[1], nodes[2]
			migrateProcessUser(t, nodes, "u2", oldLeader.config.ID)
			sender := newLeader.connect("u1")
			var existingMessages <-chan string
			if tc.localDevice {
				existingMessages = receiveProcessMessage(t, newLeader.connect("u2"))
			}
			oldLeader.call(clusterProcessRequest{Op: "arm-connack", UID: "u2"})
			receiver := client.New(owner.config.TCPAddr, client.WithUID("u2"), func(opts *client.Options) error {
				// 门控期间等待真实迁移和恢复，不以客户端握手超时或心跳掩盖竞态。
				opts.Timeout = 30 * time.Second
				opts.PingInterval = 0
				return nil
			})
			t.Cleanup(receiver.Close)
			// 后登记先执行；失败时先放行门控，再关闭正在握手的客户端。
			t.Cleanup(func() { oldLeader.call(clusterProcessRequest{Op: "release-connack"}) })
			messages := receiveProcessMessage(t, receiver)
			connected := make(chan error, 1)
			go func() { connected <- receiver.Connect() }()
			captured := oldLeader.call(clusterProcessRequest{Op: "wait-connack"})
			require.Equal(t, owner.config.ID, captured.NodeID)
			require.NotZero(t, captured.ConnID)
			require.NotEmpty(t, captured.SessionID)
			owner.call(clusterProcessRequest{
				Op: "hold-connack-socket", UID: "u2", ConnID: captured.ConnID, SessionID: captured.SessionID,
			})
			migrateProcessUser(t, nodes, "u2", newLeader.config.ID)
			recoveryLeader := newLeader
			if tc.returnToOld {
				// 领导 ID 虽回到认证时的旧值，配置代次已改变，旧认证目录仍不能复用。
				migrateProcessUser(t, nodes, "u2", oldLeader.config.ID)
				recoveryLeader = oldLeader
			}
			before := recoveryLeader.call(clusterProcessRequest{Op: "ensure-recovery", UID: "u2"})
			wantBefore := 0
			if tc.localDevice {
				wantBefore = 1
			}
			require.Equal(t, wantBefore, before.ConnCount, "被门控设备不能提前进入真实恢复快照")
			oldLeader.call(clusterProcessRequest{Op: "release-connack"})
			select {
			case err := <-connected:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("释放真实 Connack 后客户端仍未完成登录")
			}
			// 不等心跳、不重连、不主动再次 Ensure，登录后的第一条消息立即发送。
			const payload = "after-delayed-connack"
			require.NoError(t, sender.SendMessage(client.NewChannel("u2", 1), []byte(payload)))
			requireProcessMessage(t, messages, payload)
			if existingMessages != nil {
				requireProcessMessage(t, existingMessages, payload)
			}
			require.Equal(t, wantBefore+1, recoveryLeader.call(clusterProcessRequest{Op: "user-state", UID: "u2"}).ConnCount)
		})
	}
}
