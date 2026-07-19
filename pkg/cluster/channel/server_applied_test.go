package channel

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

var appliedTestTraceOnce sync.Once

func TestServerProposalObservesSearchWatermarkWithoutReappendingMessage(t *testing.T) {
	base := openAppliedTestDB(t)
	db := newCountingAppendDB(base)
	server := newAppliedTestServer(t, db)
	if !server.raftGroups[0].Options().NotNeedApplied {
		t.Fatal("channel Raft core apply path was enabled")
	}

	proposeAppliedTestMessage(t, server, "durable-applied", 1)
	if got := db.appendCalls.Load(); got != 1 {
		t.Fatalf("AppendMessages calls = %d, want exactly one physical write", got)
	}
	db.waitForUpdate(t)
	applied, err := db.GetChannelAppliedIndex("durable-applied", 2)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 1 {
		t.Fatalf("durable search watermark = %d, want 1", applied)
	}
}

func TestServerProposalSucceedsWhenSearchWatermarkObserverFails(t *testing.T) {
	base := openAppliedTestDB(t)
	db := newCountingAppendDB(base)
	db.failUpdates.Store(true)
	server := newAppliedTestServer(t, db)

	proposeAppliedTestMessage(t, server, "observer-failure", 1)
	db.waitForUpdate(t)
	if got := db.appendCalls.Load(); got != 1 {
		t.Fatalf("AppendMessages calls = %d, want exactly one physical write", got)
	}
	applied, err := base.GetChannelAppliedIndex("observer-failure", 2)
	if err != nil {
		t.Fatal(err)
	}
	physical, _, err := base.GetChannelLastMessageSeq("observer-failure", 2)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 || physical != 1 {
		t.Fatalf("watermarks after observer failure = applied:%d physical:%d, want 0/1", applied, physical)
	}
}

func openAppliedTestDB(t *testing.T) wkdb.DB {
	t.Helper()
	appliedTestTraceOnce.Do(func() {
		trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions(
			trace.WithServiceName("channel-search-applied-test"),
			trace.WithServiceHostName("test"),
		)))
	})
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})
	return db
}

func newAppliedTestServer(t *testing.T, db wkdb.DB) *Server {
	t.Helper()
	server := NewServer(NewOptions(
		WithNodeId(1),
		WithGroupCount(1),
		WithDB(db),
		WithTransport(discardChannelTransport{}),
	))
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Stop)
	return server
}

func proposeAppliedTestMessage(t *testing.T, server *Server, channelID string, messageID uint64) {
	t.Helper()
	const channelType = uint8(2)
	if err := server.WakeLeaderIfNeed(wkdb.ChannelClusterConfig{
		Id: 1, ChannelId: channelID, ChannelType: channelType,
		Replicas: []uint64{1}, LeaderId: 1, Term: 1, ConfVersion: 1,
	}); err != nil {
		t.Fatal(err)
	}
	message := wkdb.Message{RecvPacket: wkproto.RecvPacket{
		MessageID: int64(messageID), ClientMsgNo: "proposal", ChannelID: channelID,
		ChannelType: channelType, Payload: []byte(`{"type":1,"content":"hello"}`),
	}}
	data, err := message.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	responses, err := server.ProposeBatchUntilAppliedTimeoutForLocal(ctx, channelID, channelType, types.ProposeReqSet{{Id: messageID, Data: data}})
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 1 || responses[0].Index != 1 {
		t.Fatalf("responses = %#v, want one successful proposal at index 1", responses)
	}
}

type countingAppendDB struct {
	wkdb.DB
	appendCalls atomic.Int32
	failUpdates atomic.Bool
	updates     chan struct{}
}

func newCountingAppendDB(db wkdb.DB) *countingAppendDB {
	return &countingAppendDB{DB: db, updates: make(chan struct{}, 8)}
}

func (d *countingAppendDB) AppendMessages(channelID string, channelType uint8, messages []wkdb.Message) error {
	d.appendCalls.Add(1)
	return d.DB.AppendMessages(channelID, channelType, messages)
}

func (d *countingAppendDB) UpdateChannelAppliedIndex(channelID string, channelType uint8, index uint64) error {
	var err error
	if d.failUpdates.Load() {
		err = errors.New("injected search watermark failure")
	} else {
		err = d.DB.UpdateChannelAppliedIndex(channelID, channelType, index)
	}
	select {
	case d.updates <- struct{}{}:
	default:
	}
	return err
}

func (d *countingAppendDB) waitForUpdate(t *testing.T) {
	t.Helper()
	select {
	case <-d.updates:
	case <-time.After(5 * time.Second):
		t.Fatal("search watermark observer did not run")
	}
}

type discardChannelTransport struct{}

func (discardChannelTransport) Send(string, types.Event) {}
