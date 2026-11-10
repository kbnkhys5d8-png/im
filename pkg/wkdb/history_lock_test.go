package wkdb

import (
	"fmt"
	"sync"
	"testing"
	"time"

	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
)

type historySlowBatch struct {
	physicalBatch
	entered chan struct{}
	resume  chan struct{}
	once    *sync.Once
}

func (b *historySlowBatch) Commit(options *pebble.WriteOptions) error {
	b.once.Do(func() { close(b.entered) })
	<-b.resume
	return b.physicalBatch.Commit(options)
}

func TestHistorySlowShardCommitDoesNotBlockOtherChannels(t *testing.T) {
	db := NewWukongDB(NewOptions(WithDir(t.TempDir()), WithShardNum(2))).(*wukongDB)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	fence := HistoryFence{TaskID: fmt.Sprintf("%064x", 1), SourceUID: "a", PeerUID: "c", ChannelID: "a@c"}
	slow := "slow@peer"
	for i := 0; db.GetChannelShardIndex(slow, 1) == db.GetChannelShardIndex(fence.ChannelID, 1); i++ {
		slow = fmt.Sprintf("slow-%d@peer", i)
	}
	group := "synthetic-group"
	for i := 0; db.GetChannelShardIndex(group, 2) == db.GetChannelShardIndex(slow, 1); i++ {
		group = fmt.Sprintf("synthetic-group-%d", i)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	var once, resumeOnce sync.Once
	batchDB := db.channelBatchDb(slow, 1)
	makeBatch := batchDB.newPhysicalBatch
	batchDB.newPhysicalBatch = func() physicalBatch {
		return &historySlowBatch{physicalBatch: makeBatch(), entered: entered, resume: resume, once: &once}
	}
	var workers sync.WaitGroup
	unblock := func() { resumeOnce.Do(func() { close(resume) }) }
	defer func() { unblock(); workers.Wait() }()
	message := func(channel string, kind uint8) Message {
		return Message{RecvPacket: wkproto.RecvPacket{MessageID: 1, MessageSeq: 1, ChannelID: channel, ChannelType: kind, Payload: []byte(`{"type":1}`)}}
	}
	slowDone := make(chan error, 1)
	workers.Add(1)
	go func() { defer workers.Done(); slowDone <- db.AppendMessages(slow, 1, []Message{message(slow, 1)}) }()
	<-entered
	frozen, groupDone := make(chan error, 1), make(chan error, 1)
	workers.Add(1)
	go func() { defer workers.Done(); _, err := db.FreezeHistoryFence(fence); frozen <- err }()
	select {
	case err := <-frozen:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("无关私聊的慢提交阻塞了另一频道冻结")
	}
	workers.Add(1)
	go func() { defer workers.Done(); groupDone <- db.AppendMessages(group, 2, []Message{message(group, 2)}) }()
	select {
	case err := <-groupDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("无关群聊被迁移门禁阻塞")
	}
	unblock()
	if err := <-slowDone; err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	db.historyFenceLocks.mu.Lock()
	defer db.historyFenceLocks.mu.Unlock()
	if len(db.historyFenceLocks.entries) != 0 {
		t.Fatal("空闲频道锁没有回收")
	}
}
