package event

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

type stopTestEvent struct{}

func (stopTestEvent) OnSlotElection([]*clustertypes.Slot) error { return nil }

type dropStopTestTransport struct{}

func (dropStopTestTransport) Send(rafttypes.Event) {}

func newNoQuorumConfigServer(t *testing.T) (*clusterconfig.Server, *clusterconfig.Options) {
	t.Helper()
	// 三个投票副本中只启动本节点，第一笔超时后仍有另一个节点可被错误地继续提案。
	configPath := filepath.Join(t.TempDir(), "cluster.json")
	configBytes, err := json.Marshal(&clustertypes.Config{
		Nodes: []*clustertypes.Node{
			{Id: 1, Online: true, AllowVote: true, Role: clustertypes.NodeRole_NodeRoleReplica, Status: clustertypes.NodeStatus_NodeStatusJoined},
			{Id: 2, Online: true, AllowVote: true, Role: clustertypes.NodeRole_NodeRoleReplica, Status: clustertypes.NodeStatus_NodeStatusJoined},
			{Id: 3, Online: true, AllowVote: true, Role: clustertypes.NodeRole_NodeRoleReplica, Status: clustertypes.NodeStatus_NodeStatusJoined},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	opts := clusterconfig.NewOptions(
		clusterconfig.WithNodeId(1),
		clusterconfig.WithConfigPath(configPath),
		clusterconfig.WithTransport(dropStopTestTransport{}),
		clusterconfig.WithPongMaxTick(1),
	)
	cfgServer := clusterconfig.New(opts)
	if err := cfgServer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cfgServer.Stop)
	cfgServer.StepRaftEvent(rafttypes.Event{
		Type: rafttypes.ConfChange,
		Config: rafttypes.Config{
			Replicas: []uint64{1, 2, 3},
			Leader:   1,
			Term:     2,
		},
	})
	deadline := time.Now().Add(2 * time.Second)
	for !cfgServer.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("配置 Raft 未成为测试领导者")
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cfgServer, opts
}

func waitStopTestLocalLog(t *testing.T, cfgServer *clusterconfig.Server) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		logs, err := cfgServer.GetLogsByLimit(1, 0, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(logs) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("提案未进入本地日志")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStopDoesNotStartNextOnlineStatusProposal(t *testing.T) {
	cfgServer, opts := newNoQuorumConfigServer(t)
	eventServer := NewServer(stopTestEvent{}, opts, cfgServer)
	if err := eventServer.Start(); err != nil {
		t.Fatal(err)
	}
	var stopOnce sync.Once
	stopEvent := func() { stopOnce.Do(eventServer.Stop) }
	t.Cleanup(stopEvent)
	// 等待本地持久化确认正在执行提案，而不是在提案开始前抢先停机。
	waitStopTestLocalLog(t, cfgServer)

	started := time.Now()
	stopped := make(chan struct{})
	go func() {
		stopEvent()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(12 * time.Second):
		t.Fatal("节点事件服务停机超过 12 秒")
	}
	if elapsed := time.Since(started); elapsed < time.Second || elapsed >= 7*time.Second {
		t.Fatalf("停机应等待当前提案自然超时且不处理下一个节点：%s", elapsed)
	}
	logs, err := cfgServer.GetLogsByLimit(1, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("停机后配置日志有 %d 条，预期仅首笔", len(logs))
	}
	var cmd clusterconfig.CMD
	if err := cmd.Unmarshal(logs[0].Data); err != nil {
		t.Fatal(err)
	}
	if cmd.CmdType != clusterconfig.CMDTypeNodeOnlineStatusChange {
		t.Fatalf("首笔提案类型为 %s", cmd.CmdType)
	}
	nodeID, online, err := clusterconfig.DecodeNodeOnlineStatusChange(cmd.Data)
	if err != nil {
		t.Fatal(err)
	}
	if nodeID != 2 || online {
		t.Fatalf("首笔提案为节点 %d 在线=%t，预期仅节点 2 下线", nodeID, online)
	}
}
