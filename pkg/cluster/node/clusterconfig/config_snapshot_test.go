package clusterconfig

import (
	"encoding/json"
	"sync"
	"testing"

	pb "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func snapshotTestConfig() *pb.Config {
	return &pb.Config{
		Term: 1, Version: 1, Learners: []uint64{2}, MigrateFrom: 2, MigrateTo: 2,
		Nodes: []*pb.Node{{Id: 1, AllowVote: true, ApiServerAddr: "old", Role: pb.NodeRole_NodeRoleReplica}},
		Slots: []*pb.Slot{{Id: 0, Leader: 1, Replicas: []uint64{1}, Learners: []uint64{2}}},
	}
}

func TestConfigReadSnapshotsDoNotAlias(t *testing.T) {
	t.Run("full_config", func(t *testing.T) {
		for _, public := range []bool{false, true} {
			cfg := &Config{cfg: snapshotTestConfig()}
			s := &Server{config: cfg}
			var snapshot *pb.Config
			if public {
				snapshot = s.GetClusterConfig()
			} else {
				snapshot = cfg.config()
			}
			snapshot.Term = 9
			snapshot.Learners[0] = 9
			snapshot.Nodes[0].ApiServerAddr = "changed"
			snapshot.Slots[0].Replicas[0] = 9
			require.True(t, proto.Equal(snapshotTestConfig(), cfg.cfg), "返回配置不应改变内部状态")
		}
	})
	t.Run("slots", func(t *testing.T) {
		cfg := &Config{cfg: snapshotTestConfig()}
		s := &Server{config: cfg}
		slots := s.Slots()
		slots[0].Replicas[0] = 9
		slots[0].Learners[0] = 9
		slots[0] = &pb.Slot{Id: 9}
		require.True(t, proto.Equal(snapshotTestConfig(), cfg.cfg), "返回槽不应改变内部状态")
	})
	t.Run("node", func(t *testing.T) {
		cfg := &Config{cfg: snapshotTestConfig()}
		s := &Server{config: cfg}
		node := s.Node(1)
		node.ApiServerAddr = "changed"
		require.True(t, proto.Equal(snapshotTestConfig(), cfg.cfg), "返回节点不应改变内部状态")
		require.Nil(t, s.Node(99))
	})
	t.Run("raft_config", func(t *testing.T) {
		cfg := &Config{cfg: snapshotTestConfig()}
		s := &Server{opts: NewOptions(WithNodeId(1)), config: cfg}
		snapshot := s.configToRaftConfig(cfg)
		require.Equal(t, []uint64{1}, snapshot.Replicas)
		require.Equal(t, uint64(1), snapshot.Leader)
		snapshot.Learners[0] = 9
		require.True(t, proto.Equal(snapshotTestConfig(), cfg.cfg), "Raft 配置不应改变内部状态")
	})
}

func TestConfigReadSnapshotsRemainStable(t *testing.T) {
	cfg := &Config{cfg: snapshotTestConfig()}
	s := &Server{config: cfg}
	full := s.GetClusterConfig()
	slots := s.Slots()
	node := s.Node(1)

	cfg.updateSlotMigrate(0, 1, 3)
	cfg.updateApiServerAddr(1, "new")
	cfg.updateNodeJoining(2)
	cfg.updateSlots([]*pb.Slot{{Id: 0, Leader: 2, Replicas: []uint64{2}}})

	want := snapshotTestConfig()
	require.True(t, proto.Equal(want, full), "旧配置快照应保持不变")
	require.Len(t, slots, 1)
	require.True(t, proto.Equal(want.Slots[0], slots[0]), "旧槽快照应保持不变")
	require.True(t, proto.Equal(want.Nodes[0], node), "旧节点快照应保持不变")
}

func TestConfigSnapshotConcurrentApply(t *testing.T) {
	opts := NewOptions(WithNodeId(1), WithConfigPath(t.TempDir()+"/cluster.json"))
	cfg := NewConfig(opts)
	t.Cleanup(func() { require.NoError(t, cfg.cfgFile.Close()) })
	cfg.update(snapshotTestConfig())
	s := &Server{opts: opts, config: cfg}
	nodeData, err := (&pb.Node{Id: 2, AllowVote: true, Role: pb.NodeRole_NodeRoleReplica}).Marshal()
	require.NoError(t, err)
	joinData, err := NewCMD(CMDTypeNodeJoin, nodeData).Marshal()
	require.NoError(t, err)
	migrateData, err := EncodeMigrateSlot(0, 1, 3)
	require.NoError(t, err)
	migrateData, err = NewCMD(CMDTypeSlotMigrate, migrateData).Marshal()
	require.NoError(t, err)

	started := make(chan struct{})
	done := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		close(started)
		for {
			select {
			case <-done:
				return
			default:
			}
			// 模拟准备检查、API 读取和进程测试在 getter 返回后继续读取配置。
			full := s.GetClusterConfig()
			_, _ = json.Marshal(full)
			_, _ = json.Marshal(s.Slots())
			_, _ = json.Marshal(s.Node(1))
			_ = cfg.term()
			_ = cfg.version()
			_, _ = json.Marshal(s.configToRaftConfig(cfg))
		}
	}()
	<-started
	defer func() {
		close(done)
		readers.Wait()
	}()
	for i := uint64(1); i <= 200; i++ {
		cfg.updateNodeJoining(2)
		require.NoError(t, s.applyLog(rafttypes.Log{Index: i * 2, Term: uint32(i), Data: joinData}))
		cfg.updateSlots([]*pb.Slot{{Id: 0, Leader: 1, Replicas: []uint64{1}}})
		require.NoError(t, s.applyLog(rafttypes.Log{Index: i*2 + 1, Term: uint32(i), Data: migrateData}))
		cfg.updateApiServerAddr(1, "new")
	}
	require.Equal(t, uint64(401), s.GetClusterConfig().Version)
	require.Equal(t, uint32(200), s.GetClusterConfig().Term)
}
