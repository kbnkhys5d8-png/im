package cluster

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	clustertypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

func TestRecoverSearchOutboxVisitsOnlyPendingChannelsAndWakesLocalRafts(t *testing.T) {
	db := newSearchOutboxRecoveryTestDB(
		wkdb.Channel{ChannelId: "pending-a", ChannelType: 2},
		wkdb.Channel{ChannelId: "pending-b", ChannelType: 2},
	)
	db.configs["pending-a"] = recoveryTestConfig("pending-a", 1, []uint64{1})
	db.configs["pending-b"] = recoveryTestConfig("pending-b", 2, []uint64{1, 2})
	followerCluster := &searchOutboxRecoveryCluster{configs: db.configs}
	server := newSearchOutboxRecoveryServer(t, db, followerCluster)

	if err := server.SearchOutboxReady(); err == nil {
		t.Fatal("search outbox reported ready before recovery")
	}
	if err := server.RecoverSearchOutbox(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := server.SearchOutboxReady(); err != nil {
		t.Fatalf("search outbox readiness = %v, want ready", err)
	}
	if got := fmt.Sprint(db.authorityCalls); got != "[pending-a pending-b]" {
		t.Fatalf("authority reads = %s, want [pending-a pending-b]", got)
	}
	if db.inventoryCalls != 0 {
		t.Fatalf("channel inventory reads = %d, want 0", db.inventoryCalls)
	}
	for _, channelID := range []string{"pending-a", "pending-b"} {
		if !server.channelServer.ExistChannel(channelID, 2) {
			t.Fatalf("pending channel %q was not woken", channelID)
		}
	}
}

func TestRecoverSearchOutboxFailureLeavesReadinessUnavailable(t *testing.T) {
	injected := errors.New("injected recovery failure")
	tests := []struct {
		name    string
		prepare func(*searchOutboxRecoveryTestDB, *searchOutboxRecoveryCluster) context.Context
	}{
		{
			name: "cancellation",
			prepare: func(*searchOutboxRecoveryTestDB, *searchOutboxRecoveryCluster) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
		},
		{
			name: "missing authority",
			prepare: func(db *searchOutboxRecoveryTestDB, _ *searchOutboxRecoveryCluster) context.Context {
				db.configErr["pending"] = wkdb.ErrNotFound
				return context.Background()
			},
		},
		{
			name: "local node absent",
			prepare: func(db *searchOutboxRecoveryTestDB, _ *searchOutboxRecoveryCluster) context.Context {
				db.configs["pending"] = recoveryTestConfig("pending", 2, []uint64{2})
				return context.Background()
			},
		},
		{
			name: "leader wake failure",
			prepare: func(db *searchOutboxRecoveryTestDB, _ *searchOutboxRecoveryCluster) context.Context {
				db.lastErr["pending"] = injected
				return context.Background()
			},
		},
		{
			name: "follower wake failure",
			prepare: func(db *searchOutboxRecoveryTestDB, follower *searchOutboxRecoveryCluster) context.Context {
				cfg := recoveryTestConfig("pending", 2, []uint64{1, 2})
				db.configs["pending"] = cfg
				follower.configs["pending"] = cfg
				follower.err = injected
				return context.Background()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := newSearchOutboxRecoveryTestDB(
				wkdb.Channel{ChannelId: "pending", ChannelType: 2},
			)
			db.configs["pending"] = recoveryTestConfig("pending", 1, []uint64{1})
			followerCluster := &searchOutboxRecoveryCluster{configs: db.configs}
			ctx := test.prepare(db, followerCluster)
			server := newSearchOutboxRecoveryServer(t, db, followerCluster)

			if err := server.RecoverSearchOutbox(ctx); err == nil {
				t.Fatal("recovery succeeded, want failure")
			}
			if err := server.SearchOutboxReady(); err == nil {
				t.Fatal("search outbox reported ready after recovery failure")
			}
			if db.inventoryCalls != 0 {
				t.Fatalf("channel inventory reads = %d, want 0", db.inventoryCalls)
			}
		})
	}
}

func newSearchOutboxRecoveryServer(
	t *testing.T,
	db *searchOutboxRecoveryTestDB,
	followerCluster *searchOutboxRecoveryCluster,
) *Server {
	t.Helper()
	configOptions := clusterconfig.NewOptions(
		clusterconfig.WithNodeId(1),
		clusterconfig.WithSlotCount(1),
		clusterconfig.WithConfigPath(t.TempDir()+"/cluster.json"),
	)
	configServer := clusterconfig.New(configOptions)
	configServer.GetClusterConfig().Slots = []*clustertypes.Slot{{
		Id: 0, Leader: 1, Replicas: []uint64{1},
	}}
	channelServer := channel.NewServer(channel.NewOptions(
		channel.WithNodeId(1),
		channel.WithGroupCount(1),
		channel.WithDB(db),
		channel.WithCluster(followerCluster),
		channel.WithTransport(searchOutboxRecoveryTransport{}),
	))
	if err := channelServer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(channelServer.Stop)

	opts := NewOptions(WithConfigOptions(configOptions))
	return &Server{
		opts:          opts,
		cfgServer:     configServer,
		db:            db,
		channelServer: channelServer,
		store: store.New(store.NewOptions(
			store.WithNodeId(1),
			store.WithDB(db),
			store.WithChannel(channelServer),
		)),
	}
}

func recoveryTestConfig(channelID string, leader uint64, replicas []uint64) wkdb.ChannelClusterConfig {
	return wkdb.ChannelClusterConfig{
		Id: 1, ChannelId: channelID, ChannelType: 2,
		Replicas: replicas, LeaderId: leader, Term: 1, ConfVersion: 1,
	}
}

type searchOutboxRecoveryTestDB struct {
	wkdb.DB
	pending        []wkdb.Channel
	configs        map[string]wkdb.ChannelClusterConfig
	configErr      map[string]error
	lastErr        map[string]error
	authorityCalls []string
	inventoryCalls int
}

func newSearchOutboxRecoveryTestDB(pending ...wkdb.Channel) *searchOutboxRecoveryTestDB {
	return &searchOutboxRecoveryTestDB{
		pending:   pending,
		configs:   make(map[string]wkdb.ChannelClusterConfig),
		configErr: make(map[string]error),
		lastErr:   make(map[string]error),
	}
}

func (d *searchOutboxRecoveryTestDB) ScanSearchOutboxChannels(
	ctx context.Context,
	visit func(wkdb.Channel) error,
) error {
	for _, pending := range d.pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(pending); err != nil {
			return err
		}
	}
	return nil
}

func (d *searchOutboxRecoveryTestDB) GetChannelClusterConfig(
	channelID string,
	_ uint8,
) (wkdb.ChannelClusterConfig, error) {
	d.authorityCalls = append(d.authorityCalls, channelID)
	if err := d.configErr[channelID]; err != nil {
		return wkdb.EmptyChannelClusterConfig, err
	}
	cfg, ok := d.configs[channelID]
	if !ok {
		return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
	}
	return cfg, nil
}

func (d *searchOutboxRecoveryTestDB) GetChannelClusterConfigs(
	uint64,
	int,
) ([]wkdb.ChannelClusterConfig, error) {
	d.inventoryCalls++
	panic("recovery must not scan the channel inventory")
}

func (d *searchOutboxRecoveryTestDB) GetLastMsg(channelID string, _ uint8) (wkdb.Message, error) {
	if err := d.lastErr[channelID]; err != nil {
		return wkdb.Message{}, err
	}
	message := wkdb.Message{Term: 1}
	message.MessageSeq = 1
	return message, nil
}

func (*searchOutboxRecoveryTestDB) GetSearchOutboxFloor(string, uint8) (uint64, bool, error) {
	return 0, true, nil
}

func (*searchOutboxRecoveryTestDB) GetChannelAppliedIndex(string, uint8) (uint64, error) {
	return 0, nil
}

func (*searchOutboxRecoveryTestDB) LeaderTermStartIndex(string, uint32) (uint64, error) {
	return 0, nil
}

type searchOutboxRecoveryCluster struct {
	icluster.ICluster
	configs map[string]wkdb.ChannelClusterConfig
	err     error
}

func (c *searchOutboxRecoveryCluster) GetOrCreateChannelClusterConfigFromSlotLeader(
	channelID string,
	_ uint8,
) (wkdb.ChannelClusterConfig, error) {
	if c.err != nil {
		return wkdb.EmptyChannelClusterConfig, c.err
	}
	cfg, ok := c.configs[channelID]
	if !ok {
		return wkdb.EmptyChannelClusterConfig, wkdb.ErrNotFound
	}
	return cfg, nil
}

type searchOutboxRecoveryTransport struct{}

func (searchOutboxRecoveryTransport) Send(string, rafttypes.Event) {}
