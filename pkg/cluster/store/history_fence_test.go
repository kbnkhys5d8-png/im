package store

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type historyTestSlot struct {
	icluster.Slot
	apply func([]byte) error
}

func (s *historyTestSlot) GetSlotId(string) uint32 { return 0 }
func (s *historyTestSlot) ProposeUntilAppliedTimeout(_ context.Context, _ uint32, data []byte) (*rafttypes.ProposeResp, error) {
	return &rafttypes.ProposeResp{}, s.apply(data)
}

func (s *historyTestSlot) ProposeUntilApplied(slot uint32, data []byte) (*rafttypes.ProposeResp, error) {
	return s.ProposeUntilAppliedTimeout(context.Background(), slot, data)
}

type historyTestChannel struct {
	db      wkdb.DB
	mu      sync.Mutex
	entered chan struct{}
	resume  chan struct{}
}

func (c *historyTestChannel) ProposeBatchUntilAppliedTimeout(_ context.Context, channel string, kind uint8, reqs rafttypes.ProposeReqSet) (rafttypes.ProposeRespSet, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entered != nil {
		close(c.entered)
		<-c.resume
		c.entered = nil
	}
	seq, _, err := c.db.GetChannelLastMessageSeq(channel, kind)
	if err != nil {
		return nil, err
	}
	var messages []wkdb.Message
	var result rafttypes.ProposeRespSet
	for i, req := range reqs {
		var msg wkdb.Message
		if err := msg.Unmarshal(req.Data); err != nil {
			return nil, err
		}
		msg.MessageSeq = uint32(seq + uint64(i) + 1)
		messages = append(messages, msg)
		result = append(result, &rafttypes.ProposeResp{Id: req.Id, Index: uint64(msg.MessageSeq)})
	}
	return result, c.db.AppendMessages(channel, kind, messages)
}

func historyTestDB(t *testing.T, path string) wkdb.DB {
	t.Helper()
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(path), wkdb.WithShardNum(1)))
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	return db
}
func historyTestStore(db wkdb.DB, channel *historyTestChannel) *Store {
	slot := &historyTestSlot{}
	s := New(NewOptions(WithDB(db), WithChannel(channel), WithSlot(slot), WithHistoryTopology(func(string, uint8) error { return nil }), WithHistoryDrain(func(context.Context, string, uint8) error { return nil }), WithHistorySourceChannel(func(ch string) string { return strings.TrimSuffix(ch, "&cmd") })))
	slot.apply = func(data []byte) error {
		cmd := &CMD{}
		if err := cmd.Unmarshal(data); err != nil {
			return err
		}
		return s.applyHistoryFence(cmd)
	}
	return s
}
func historyIdentity() wkdb.HistoryFence {
	return wkdb.HistoryFence{TaskID: strings.Repeat("a", 64), SourceUID: "a", PeerUID: "c", ChannelID: "a@c"}
}
func historyMessage(channel string, id int64) wkdb.Message {
	return wkdb.Message{RecvPacket: wkproto.RecvPacket{ChannelID: channel, ChannelType: 1, MessageID: id, FromUID: "a", ClientMsgNo: "history-client", Payload: []byte(`{"type":1,"content":"合成"}`)}}
}
func historyMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestHistoryFreezeWaitsForAppendAndSurvivesRestart(t *testing.T) {
	path := t.TempDir()
	db := historyTestDB(t, path)
	c := &historyTestChannel{db: db, entered: make(chan struct{}), resume: make(chan struct{})}
	s := historyTestStore(db, c)
	appendDone := make(chan error, 1)
	go func() {
		_, err := s.AppendMessages(context.Background(), "a@c", 1, []wkdb.Message{historyMessage("a@c", 100)})
		appendDone <- err
	}()
	<-c.entered
	frozen := make(chan error, 1)
	go func() {
		f, err := s.FreezeHistory(context.Background(), historyIdentity())
		if err == nil && f.HighWatermark != 1 {
			err = errors.New("水位未包含已接收提案")
		}
		frozen <- err
	}()
	select {
	case err := <-frozen:
		t.Fatalf("提案未完成就 freeze: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(c.resume)
	historyMust(t, <-appendDone)
	historyMust(t, <-frozen)
	_, err := s.AppendMessages(context.Background(), "a@c", 1, []wkdb.Message{historyMessage("a@c", 101)})
	if !errors.Is(err, wkdb.ErrHistoryFenced) {
		t.Fatal(err)
	}
	historyMust(t, db.Close())
	db = historyTestDB(t, path)
	defer db.Close()
	s = historyTestStore(db, &historyTestChannel{db: db})
	f, err := s.FreezeHistory(context.Background(), historyIdentity())
	historyMust(t, err)
	if f.HighWatermark != 1 {
		t.Fatal(f)
	}
	if err := db.AppendMessages("a@c", 1, []wkdb.Message{historyMessage("a@c", 102)}); !errors.Is(err, wkdb.ErrHistoryFenced) {
		t.Fatal(err)
	}
	page, err := s.ExportHistory(context.Background(), historyIdentity(), 0, 1)
	historyMust(t, err)
	if len(page.Messages) != 1 || page.Messages[0].MessageID != 100 || page.More {
		t.Fatal(page)
	}
}

func TestHistoryReadersReleaseAndUnrelatedTraffic(t *testing.T) {
	db := historyTestDB(t, t.TempDir())
	defer db.Close()
	s := historyTestStore(db, &historyTestChannel{db: db})
	for i, ch := range []string{"a@c", "b@c"} {
		_, err := s.AppendMessages(context.Background(), ch, 1, []wkdb.Message{historyMessage(ch, int64(100-i))})
		historyMust(t, err)
	}
	_, err := s.FreezeHistory(context.Background(), historyIdentity())
	historyMust(t, err)
	if _, err := s.SaveChannelClusterConfig(wkdb.ChannelClusterConfig{ChannelId: "a@c", ChannelType: 1, LeaderId: 2}); !errors.Is(err, wkdb.ErrHistoryFenced) {
		t.Fatal("冻结来源仍可转移leader", err)
	}
	visible, err := s.SearchMessages(wkdb.MessageSearchReq{Limit: 1})
	historyMust(t, err)
	if len(visible) != 1 || visible[0].ChannelID != "b@c" {
		t.Fatal("过滤来源导致正常全局搜索断页", visible)
	}
	for name, read := range map[string]func() error{
		"next":     func() error { _, e := s.LoadNextRangeMsgs("a@c", 1, 1, 0, 10); return e },
		"previous": func() error { _, e := s.LoadPrevRangeMsgs("a@c", 1, 0, 0, 10); return e },
		"last":     func() error { _, e := s.LoadLastMsgs("a@c", 1, 10); return e },
		"last-end": func() error { _, e := s.LoadLastMsgsWithEnd("a@c", 1, 0, 10); return e },
		"single":   func() error { _, e := s.LoadMsg("a@c", 1, 1); return e },
		"client":   func() error { _, e := s.LoadMsgByClientMsgNo("a@c", 1, "history-client"); return e },
		"max":      func() error { _, e := s.GetLastMsgSeq("a@c", 1); return e },
		"sender":   func() error { _, e := s.GetUserLastMsgSeq("a", "a@c", 1); return e },
		"events":   func() error { _, e := s.GetMessageEventStates("a@c", 1, "history-client"); return e },
	} {
		t.Run(name, func(t *testing.T) {
			if !errors.Is(read(), wkdb.ErrHistoryFenced) {
				t.Fatal("来源读取未隐藏")
			}
		})
	}
	for _, req := range []wkdb.MessageSearchReq{{Limit: 20}, {MessageId: 100}, {FromUid: "a", Limit: 20}, {ClientMsgNo: "history-client", Limit: 20}} {
		rows, err := s.SearchMessages(req)
		historyMust(t, err)
		for _, m := range rows {
			if m.ChannelID == "a@c" {
				t.Fatal("全局索引泄露")
			}
		}
	}
	batch, err := s.LoadMsgsBatch([]wkdb.BatchMsgRequest{{ChannelId: "a@c", ChannelType: 1, MsgSeq: 1, Limit: 10}, {ChannelId: "b@c", ChannelType: 1, MsgSeq: 1, Limit: 10}})
	historyMust(t, err)
	if len(batch) != 2 || len(batch[0].Messages) != 0 || len(batch[1].Messages) != 1 {
		t.Fatal(batch)
	}
	called := false
	for _, ch := range []string{"a@c", "a@c&cmd"} {
		if !errors.Is(s.WithHistorySend(ch, 1, func() error { called = true; return nil }), wkdb.ErrHistoryFenced) {
			t.Fatal("NoPersist 绕过门禁")
		}
	}
	if called {
		t.Fatal("已冻结消息仍分发")
	}
	historyMust(t, s.WithHistorySend("b@c", 1, func() error { return nil }))
	_, err = s.AppendMessages(context.Background(), "b@c", 1, []wkdb.Message{historyMessage("b@c", 200)})
	historyMust(t, err)
	wrong := historyIdentity()
	wrong.TaskID = strings.Repeat("b", 64)
	_, err = s.FreezeHistory(context.Background(), wrong)
	if !errors.Is(err, wkdb.ErrHistoryConflict) {
		t.Fatal(err)
	}
	if !errors.Is(s.ReleaseHistory(context.Background(), wrong), wkdb.ErrHistoryConflict) {
		t.Fatal("其他任务释放成功")
	}
	historyMust(t, s.ReleaseHistory(context.Background(), historyIdentity()))
	historyMust(t, s.ReleaseHistory(context.Background(), historyIdentity()))
	rows, err := s.LoadLastMsgs("a@c", 1, 10)
	historyMust(t, err)
	if len(rows) != 1 || rows[0].MessageID != 100 {
		t.Fatal(rows)
	}
	_, err = s.AppendMessages(context.Background(), "a@c", 1, []wkdb.Message{historyMessage("a@c", 201)})
	historyMust(t, err)
}

type historyTopology struct {
	nodes        []*nodetypes.Node
	leader, slot uint64
	err          error
}

func (c historyTopology) Nodes() []*nodetypes.Node { return c.nodes }
func (c historyTopology) Slots() []*nodetypes.Slot { return []*nodetypes.Slot{{Leader: c.slot}} }
func (c historyTopology) LeaderOfChannelForRead(string, uint8) (*nodetypes.Node, error) {
	return &nodetypes.Node{Id: c.leader}, c.err
}
func (c historyTopology) SlotLeaderIdOfChannel(string, uint8) (uint64, error) { return c.slot, c.err }

func TestHistoryStrictTopologyAndReplicatedFence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		topology historyTopology
		allow    bool
	}{
		{"single", historyTopology{nodes: []*nodetypes.Node{{Id: 1}}, leader: 1, slot: 1}, true},
		{"empty", historyTopology{}, false},
		{"two-members-even-offline", historyTopology{nodes: []*nodetypes.Node{{Id: 1}, {Id: 2}}, leader: 1, slot: 1}, false},
		{"foreign-node", historyTopology{nodes: []*nodetypes.Node{{Id: 2}}, leader: 1, slot: 1}, false},
		{"foreign-leader", historyTopology{nodes: []*nodetypes.Node{{Id: 1}}, leader: 2, slot: 1}, false},
		{"foreign-slot", historyTopology{nodes: []*nodetypes.Node{{Id: 1}}, leader: 1, slot: 2}, false},
		{"lookup-error", historyTopology{nodes: []*nodetypes.Node{{Id: 1}}, leader: 1, slot: 1, err: errors.New("synthetic")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := SingleNodeHistoryTopology(1, tc.topology)("a@c", 1)
			if (err == nil) != tc.allow {
				t.Fatal(err)
			}
		})
	}
	// 模拟槽日志在另一个独立 Pebble 应用；不宣称进行了真实多节点验收。
	first := historyTestDB(t, t.TempDir())
	defer first.Close()
	second := historyTestDB(t, t.TempDir())
	defer second.Close()
	s := historyTestStore(first, &historyTestChannel{db: first})
	other := historyTestStore(second, &historyTestChannel{db: second})
	slot := s.opts.Slot.(*historyTestSlot)
	localApply := slot.apply
	slot.apply = func(data []byte) error {
		if err := localApply(data); err != nil {
			return err
		}
		return other.ApplySlotLogs(0, []rafttypes.Log{{Index: 1, Data: data}})
	}
	_, err := s.FreezeHistory(context.Background(), historyIdentity())
	historyMust(t, err)
	_, err = other.AppendMessages(context.Background(), "a@c", 1, []wkdb.Message{historyMessage("a@c", 1)})
	if !errors.Is(err, wkdb.ErrHistoryFenced) {
		t.Fatal(err)
	}
	s.opts.HistoryTopology = func(string, uint8) error { return wkdb.ErrHistoryUnsupported }
	_, err = s.ExportHistory(context.Background(), historyIdentity(), 0, 1)
	if !errors.Is(err, wkdb.ErrHistoryUnsupported) {
		t.Fatal(err)
	}
	_, err = s.FreezeHistory(context.Background(), wkdb.HistoryFence{ChannelID: "b@c"})
	if !errors.Is(err, wkdb.ErrHistoryUnsupported) {
		t.Fatal(err)
	}
	_, err = other.AppendMessages(context.Background(), "b@c", 1, []wkdb.Message{historyMessage("b@c", 2)})
	historyMust(t, err)
}

func TestHistoryExportCursorAndMaximumSequence(t *testing.T) {
	db := historyTestDB(t, t.TempDir())
	defer db.Close()
	message := historyMessage("a@c", 9)
	message.MessageSeq = math.MaxUint32
	historyMust(t, db.AppendMessages("a@c", 1, []wkdb.Message{message}))
	s := historyTestStore(db, &historyTestChannel{db: db})
	_, err := s.FreezeHistory(context.Background(), historyIdentity())
	historyMust(t, err)
	page, err := s.ExportHistory(context.Background(), historyIdentity(), math.MaxUint32-1, 1)
	historyMust(t, err)
	if len(page.Messages) != 1 || page.More {
		t.Fatal(page)
	}
	page, err = s.ExportHistory(context.Background(), historyIdentity(), math.MaxUint32, 1)
	historyMust(t, err)
	if len(page.Messages) != 0 || page.More {
		t.Fatal(page)
	}
	for _, after := range []uint64{math.MaxUint32 + 1, math.MaxUint64} {
		if _, err := s.ExportHistory(context.Background(), historyIdentity(), after, 1); err == nil {
			t.Fatal("溢出游标获准")
		}
	}
	for _, limit := range []int{0, 201} {
		if _, err := s.ExportHistory(context.Background(), historyIdentity(), 0, limit); err == nil {
			t.Fatal("页大小获准")
		}
	}
	wrong := historyIdentity()
	wrong.SourceUID = "c"
	wrong.PeerUID = "a"
	if _, err := s.ExportHistory(context.Background(), wrong, 0, 1); !errors.Is(err, wkdb.ErrHistoryConflict) {
		t.Fatal(err)
	}
}

type historyErrorDB struct {
	wkdb.DB
	wkdb.HistoryFenceStore
}

func (db historyErrorDB) GetHistoryFence(string, uint8) (*wkdb.HistoryFence, error) {
	return nil, errors.New("synthetic fence read failure")
}

func TestHistoryFailurePathsStayClosed(t *testing.T) {
	db := historyTestDB(t, t.TempDir())
	defer db.Close()
	s := historyTestStore(db, &historyTestChannel{db: db})
	slot := s.opts.Slot.(*historyTestSlot)
	apply := slot.apply
	slot.apply = func([]byte) error { return errors.New("synthetic metadata proposal failure") }
	if _, err := s.FreezeHistory(context.Background(), historyIdentity()); err == nil {
		t.Fatal("元数据提案失败却返回成功")
	}
	if _, err := s.AppendMessages(context.Background(), "a@c", 1, []wkdb.Message{historyMessage("a@c", 100)}); !errors.Is(err, wkdb.ErrHistoryFenced) {
		t.Fatal(err)
	}
	slot.apply = apply
	f, err := s.FreezeHistory(context.Background(), historyIdentity())
	historyMust(t, err)
	if f.HighWatermark != 0 {
		t.Fatal(f)
	}
	wrapped := historyErrorDB{DB: db, HistoryFenceStore: db.(wkdb.HistoryFenceStore)}
	failed := historyTestStore(wrapped, &historyTestChannel{db: db})
	if _, err := failed.LoadLastMsgs("a@c", 1, 1); err == nil {
		t.Fatal("门禁读取失败仍可读")
	}
	called := false
	if err := failed.WithHistorySend("a@c", 1, func() error { called = true; return nil }); err == nil || called {
		t.Fatal("门禁读取失败仍分发")
	}
	if _, err := failed.AppendMessages(context.Background(), "a@c", 1, []wkdb.Message{historyMessage("a@c", 100)}); err == nil {
		t.Fatal("门禁读取失败仍写入")
	}
	if err := failed.ReleaseHistory(context.Background(), historyIdentity()); err == nil {
		t.Fatal("门禁读取失败当成无门禁取消")
	}
	absent := historyIdentity()
	absent.ChannelID = "b@c"
	absent.SourceUID = "b"
	historyMust(t, s.ReleaseHistory(context.Background(), absent))
}

func TestHistoryFreezeWaitsForNoPersistAdmission(t *testing.T) {
	db := historyTestDB(t, t.TempDir())
	defer db.Close()
	s := historyTestStore(db, &historyTestChannel{db: db})
	entered, resume := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- s.WithHistorySend("a@c", 1, func() error { close(entered); <-resume; return nil }) }()
	<-entered
	frozen := make(chan error, 1)
	go func() { _, err := s.FreezeHistory(context.Background(), historyIdentity()); frozen <- err }()
	select {
	case err := <-frozen:
		t.Fatal("NoPersist 尚未完成准入就 freeze", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(resume)
	historyMust(t, <-done)
	historyMust(t, <-frozen)
	if err := s.WithHistorySend("a@c", 1, func() error { t.Error("冻结后仍执行分发"); return nil }); !errors.Is(err, wkdb.ErrHistoryFenced) {
		t.Fatal(err)
	}
}

func TestHistoryFreezeWaitsForDrainAndAllowsNestedConfigSave(t *testing.T) {
	db := historyTestDB(t, t.TempDir())
	defer db.Close()
	c := &historyTestChannel{db: db, entered: make(chan struct{}), resume: make(chan struct{})}
	s := historyTestStore(db, c)
	drained := false
	s.opts.HistoryDrain = func(context.Context, string, uint8) error {
		drained = true
		seq, _, err := db.GetChannelLastMessageSeq("a@c", 1)
		if err != nil || seq != 1 {
			return errors.New("未排空先前提案")
		}
		return nil
	}
	// 模拟首次 AppendMessages 回调保存配置；freeze 排队后仍不能重入来源读锁。
	slot := s.opts.Slot.(*historyTestSlot)
	slot.apply = func(data []byte) error {
		cmd := &CMD{}
		if err := cmd.Unmarshal(data); err != nil {
			return err
		}
		if cmd.CmdType == CMDChannelClusterConfigSave {
			return nil
		}
		return s.applyHistoryFence(cmd)
	}
	appendDone := make(chan error, 1)
	go func() {
		unlock := s.historyReadLock("a@c", 1)
		defer unlock()
		close(c.entered)
		<-c.resume
		_, err := s.SaveChannelClusterConfig(wkdb.ChannelClusterConfig{ChannelId: "a@c", ChannelType: 1, LeaderId: 1})
		if err == nil {
			msg := historyMessage("a@c", 601)
			msg.MessageSeq = 1
			err = db.AppendMessages("a@c", 1, []wkdb.Message{msg})
		}
		appendDone <- err
	}()
	<-c.entered
	frozen := make(chan error, 1)
	go func() { _, err := s.FreezeHistory(context.Background(), historyIdentity()); frozen <- err }()
	time.Sleep(25 * time.Millisecond)
	close(c.resume)
	select {
	case err := <-appendDone:
		historyMust(t, err)
	case <-time.After(time.Second):
		t.Fatal("配置保存与冻结发生锁重入死锁")
	}
	historyMust(t, <-frozen)
	if !drained {
		t.Fatal("冻结未调用提案屏障")
	}
}

func TestHistoryDrainFailureCreatesNoFence(t *testing.T) {
	db := historyTestDB(t, t.TempDir())
	defer db.Close()
	s := historyTestStore(db, &historyTestChannel{db: db})
	s.opts.HistoryDrain = func(context.Context, string, uint8) error { return context.DeadlineExceeded }
	_, err := s.FreezeHistory(context.Background(), historyIdentity())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	f, err := db.(wkdb.HistoryFenceStore).GetHistoryFence("a@c", 1)
	historyMust(t, err)
	if f != nil {
		t.Fatal("排空失败仍冻结了不完整水位")
	}
}

type historyMissingTopology struct{ historyTopology }

func (c historyMissingTopology) LeaderOfChannelForRead(string, uint8) (*nodetypes.Node, error) {
	return nil, wkdb.ErrNotFound
}

func TestHistoryMissingChannelStillRequiresSingletonAndLocalSlot(t *testing.T) {
	for _, tc := range []struct {
		topology historyTopology
		missing  bool
	}{
		{historyTopology{nodes: []*nodetypes.Node{{Id: 1}}, slot: 1}, true},
		{historyTopology{nodes: []*nodetypes.Node{{Id: 1}, {Id: 2}}, slot: 1}, false},
		{historyTopology{nodes: []*nodetypes.Node{{Id: 1}}, slot: 2}, false},
	} {
		err := SingleNodeHistoryTopology(1, historyMissingTopology{tc.topology}, wkdb.ErrNotFound)("a@c", 1)
		if errors.Is(err, wkdb.ErrHistoryChannelMissing) != tc.missing || err == nil {
			t.Fatal(tc, err)
		}
	}
}

func TestHistoryFreezeDoesNotPauseOtherChannels(t *testing.T) {
	for _, phase := range []string{"drain", "slot"} {
		t.Run(phase, func(t *testing.T) {
			db := historyTestDB(t, t.TempDir())
			defer db.Close()
			s := historyTestStore(db, &historyTestChannel{db: db})
			for i, ch := range []wkdb.Channel{{ChannelId: "a@c", ChannelType: 1}, {ChannelId: "b@c", ChannelType: 1}, {ChannelId: "a@c", ChannelType: 2}} {
				msg := historyMessage(ch.ChannelId, int64(700+i))
				msg.ChannelType = ch.ChannelType
				_, err := s.AppendMessages(context.Background(), ch.ChannelId, ch.ChannelType, []wkdb.Message{msg})
				historyMust(t, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			entered, resume := make(chan struct{}), make(chan struct{})
			var resumeOnce sync.Once
			unblock := func() { resumeOnce.Do(func() { close(resume) }) }
			var workers sync.WaitGroup
			defer func() { unblock(); cancel(); workers.Wait() }()
			block := func() error {
				close(entered)
				select {
				case <-resume:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			s.opts.HistoryDrain = func(context.Context, string, uint8) error {
				if phase == "drain" {
					return block()
				}
				return nil
			}
			s.opts.Slot.(*historyTestSlot).apply = func(data []byte) error {
				cmd := &CMD{}
				if err := cmd.Unmarshal(data); err != nil {
					return err
				}
				if cmd.CmdType == CMDChannelClusterConfigSave {
					return nil
				}
				if phase == "slot" {
					if err := block(); err != nil {
						return err
					}
				}
				return s.applyHistoryFence(cmd)
			}
			start := func(fn func() error) <-chan error {
				done := make(chan error, 1)
				workers.Add(1)
				go func() { defer workers.Done(); done <- fn() }()
				return done
			}
			frozen := start(func() error { _, err := s.FreezeHistory(ctx, historyIdentity()); return err })
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("冻结未进入阻塞阶段")
			}
			// 同一字符串的群聊也必须与来源私聊分锁；命令频道则必须与来源同锁。
			sourceOps := map[string]<-chan error{}
			for _, ch := range []string{"a@c", "a@c&cmd"} {
				channel := ch
				sourceOps["append:"+ch] = start(func() error {
					_, err := s.AppendMessages(ctx, channel, 1, []wkdb.Message{historyMessage(channel, 800)})
					return err
				})
				sourceOps["nopersist:"+ch] = start(func() error { return s.WithHistorySend(channel, 1, func() error { return nil }) })
			}
			sourceOps["read"] = start(func() error { _, err := s.LoadMsg("a@c", 1, 1); return err })
			ops := map[string]func() error{}
			for _, ch := range []wkdb.Channel{{ChannelId: "b@c", ChannelType: 1}, {ChannelId: "a@c", ChannelType: 2}} {
				channel := ch
				label := ch.ChannelId + string([]byte{ch.ChannelType})
				ops["append:"+label] = func() error {
					msg := historyMessage(channel.ChannelId, 900+int64(channel.ChannelType))
					msg.ChannelType = channel.ChannelType
					_, err := s.AppendMessages(ctx, channel.ChannelId, channel.ChannelType, []wkdb.Message{msg})
					return err
				}
				ops["nopersist:"+label] = func() error {
					return s.WithHistorySend(channel.ChannelId, channel.ChannelType, func() error { return nil })
				}
				ops["read:"+label] = func() error { _, err := s.LoadMsg(channel.ChannelId, channel.ChannelType, 1); return err }
				ops["search:"+label] = func() error {
					rows, err := s.SearchMessages(wkdb.MessageSearchReq{ChannelId: channel.ChannelId, ChannelType: channel.ChannelType, Limit: 20})
					if err == nil && len(rows) == 0 {
						return errors.New("无关频道定向搜索为空")
					}
					return err
				}
				ops["config:"+label] = func() error {
					_, err := s.SaveChannelClusterConfig(wkdb.ChannelClusterConfig{ChannelId: channel.ChannelId, ChannelType: channel.ChannelType, LeaderId: 1})
					return err
				}
			}
			ops["search"] = func() error {
				rows, err := s.SearchMessages(wkdb.MessageSearchReq{Limit: 20})
				if err != nil {
					return err
				}
				if len(rows) == 0 {
					return errors.New("无关历史被隐藏")
				}
				for _, row := range rows {
					if row.ChannelID == "a@c" && row.ChannelType == 1 {
						return errors.New("来源冻结期间全局搜索泄露")
					}
				}
				return nil
			}
			ops["search-id"] = func() error {
				// 原生按 ID 查询优先于请求频道，门禁必须按实际返回记录判断。
				rows, err := s.SearchMessages(wkdb.MessageSearchReq{ChannelId: "b@c", ChannelType: 1, MessageId: 700, Limit: 20})
				if err == nil && len(rows) != 0 {
					return errors.New("按 ID 查询绕过实际来源门禁")
				}
				return err
			}
			ops["batch"] = func() error {
				rows, err := s.LoadMsgsBatch([]wkdb.BatchMsgRequest{{ChannelId: "a@c", ChannelType: 1, MsgSeq: 1, Limit: 10}, {ChannelId: "b@c", ChannelType: 1, MsgSeq: 1, Limit: 10}})
				if err == nil && (len(rows) != 2 || len(rows[0].Messages) != 0 || len(rows[1].Messages) == 0) {
					return errors.New("批量读取未按频道隐藏来源")
				}
				return err
			}
			ops["sender-batch"] = func() error {
				rows, err := s.GetUserLastMsgSeqBatch("a", []wkdb.Channel{{ChannelId: "a@c", ChannelType: 1}, {ChannelId: "b@c", ChannelType: 1}})
				if err == nil && (len(rows) != 1 || rows["b@c:"+string([]byte{1})] == 0) {
					return errors.New("发送者批量读取未过滤来源")
				}
				return err
			}
			type result struct {
				name string
				err  error
			}
			normalDone := make(chan result, len(ops))
			for name, fn := range ops {
				workers.Add(1)
				go func(name string, fn func() error) { defer workers.Done(); normalDone <- result{name, fn()} }(name, fn)
			}
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			for range ops {
				select {
				case r := <-normalDone:
					if r.err != nil {
						t.Fatal(r.name, r.err)
					}
				case <-deadline.C:
					t.Fatal("来源冻结阻塞了无关聊天或配置保存")
				}
			}
			for name, done := range sourceOps {
				select {
				case err := <-done:
					t.Fatalf("来源操作在冻结完成前通过: %s %v", name, err)
				default:
				}
			}
			unblock()
			historyMust(t, <-frozen)
			for name, done := range sourceOps {
				if err := <-done; !errors.Is(err, wkdb.ErrHistoryFenced) {
					t.Fatal(name, err)
				}
			}
		})
	}
}
