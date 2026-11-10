package channel

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type historyBlockedAppendDB struct {
	wkdb.DB
	entered chan struct{}
	resume  chan struct{}
}

func (db *historyBlockedAppendDB) AppendMessages(ch string, kind uint8, messages []wkdb.Message) error {
	close(db.entered)
	<-db.resume
	return db.DB.AppendMessages(ch, kind, messages)
}

func TestHistoryBarrierWaitsForTimedOutAcceptedProposal(t *testing.T) {
	base := openAppliedTestDB(t)
	db := &historyBlockedAppendDB{DB: base, entered: make(chan struct{}), resume: make(chan struct{})}
	server := newAppliedTestServer(t, db)
	cfg := wkdb.ChannelClusterConfig{ChannelId: "a@c", ChannelType: 1, Replicas: []uint64{1}, LeaderId: 1, Term: 1, ConfVersion: 1}
	if err := server.WakeLeaderIfNeed(cfg); err != nil {
		t.Fatal(err)
	}
	msg := wkdb.Message{RecvPacket: wkproto.RecvPacket{MessageID: 601, ChannelID: "a@c", ChannelType: 1, Payload: []byte(`{"type":1}`)}}
	data, err := msg.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	proposed := make(chan error, 1)
	go func() {
		_, err := server.ProposeBatchUntilAppliedTimeoutForLocal(ctx, "a@c", 1, types.ProposeReqSet{{Id: 601, Data: data}})
		proposed <- err
	}()
	<-db.entered
	cancel()
	if err := <-proposed; err == nil {
		t.Fatal("未复现已接纳提案在持久化前取消返回")
	}
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer drainCancel()
	drained := make(chan error, 1)
	go func() { drained <- server.WaitForHistory(drainCtx, "a@c", 1) }()
	select {
	case err := <-drained:
		close(db.resume)
		t.Fatalf("未落盘就通过屏障: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(db.resume)
	if err := <-drained; err != nil {
		t.Fatal(err)
	}
	seq, _, err := base.GetChannelLastMessageSeq("a@c", 1)
	if err != nil || seq != 1 {
		t.Fatal(seq, err)
	}
}

func TestHistoryBarrierDoesNotCreateEmptyChannel(t *testing.T) {
	db := openAppliedTestDB(t)
	server := newAppliedTestServer(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.WaitForHistory(ctx, "a@c", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := db.GetChannelClusterConfig("a@c", 1); err != wkdb.ErrNotFound {
		t.Fatal("屏障创建了普通频道配置", err)
	}
	if server.raftGroups[0].GetRaftCount() != 0 {
		t.Fatal("空频道被唤醒")
	}
}
