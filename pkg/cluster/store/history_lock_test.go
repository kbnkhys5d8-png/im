package store

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

func TestHistoryLockWaitersAndIdleEntries(t *testing.T) {
	for _, mode := range []string{"history", "config"} {
		t.Run(mode, func(t *testing.T) {
			s := New(NewOptions(WithHistorySourceChannel(func(ch string) string { return strings.TrimSuffix(ch, "&cmd") })))
			holdLock, waitLock := s.historyWriteLock, s.historyReadLock
			if mode == "config" {
				holdLock, waitLock = s.historyConfigLock, s.historyConfigLock
			}
			held := holdLock("a@c", 1)
			key := s.historyKey("a@c", 1)
			entry := s.historyLocks.entries[key]
			readerResume, readerEntered, writerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var ownerOnce, readerOnce sync.Once
			unlockOwner := func() { ownerOnce.Do(held) }
			unlockReader := func() { readerOnce.Do(func() { close(readerResume) }) }
			var workers sync.WaitGroup
			defer func() { unlockOwner(); unlockReader(); workers.Wait() }()
			workers.Add(1)
			go func() {
				defer workers.Done()
				unlock := waitLock("a@c&cmd", 1)
				defer unlock()
				close(readerEntered)
				<-readerResume
			}()
			waitRefs := func(refs int) {
				t.Helper()
				deadline := time.NewTimer(time.Second)
				defer deadline.Stop()
				for {
					s.historyLocks.mu.Lock()
					same := s.historyLocks.entries[key] == entry
					count, size := entry.refs, len(s.historyLocks.entries)
					s.historyLocks.mu.Unlock()
					if !same || size != 1 {
						t.Fatal("同一原生/命令频道生成了第二把锁")
					}
					if count == refs {
						return
					}
					select {
					case <-deadline.C:
						t.Fatal("未将持有者及等待者计入引用", count, refs)
					case <-time.After(time.Millisecond):
					}
				}
			}
			waitRefs(2)
			unlockOwner()
			select {
			case <-readerEntered:
			case <-time.After(time.Second):
				t.Fatal("持有者释放后等待者未获得锁")
			}
			waitRefs(1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				unlock := holdLock("a@c", 1)
				unlock()
				close(writerDone)
			}()
			waitRefs(2)
			select {
			case <-writerDone:
				t.Fatal("等待者仍持锁时另一持有者已进入")
			default:
			}
			unlockReader()
			select {
			case <-writerDone:
			case <-time.After(time.Second):
				t.Fatal("等待者释放后锁未继续传递")
			}
			workers.Wait()
			for i := 0; i < 2000; i++ {
				unlock := holdLock("idle-"+strconv.Itoa(i), 1)
				unlock()
			}
			s.historyLocks.mu.Lock()
			size := len(s.historyLocks.entries)
			s.historyLocks.mu.Unlock()
			if size != 0 {
				t.Fatal("锁表长期保留已无持有者及等待者的键", size)
			}
		})
	}
}

type historyBlockedMultiReadDB struct {
	wkdb.DB
	wkdb.HistoryFenceStore
	mode    string
	entered chan struct{}
	resume  chan struct{}
}

func (db *historyBlockedMultiReadDB) pause(mode string) {
	if db.mode == mode {
		close(db.entered)
		<-db.resume
	}
}

func (db *historyBlockedMultiReadDB) SearchMessages(req wkdb.MessageSearchReq) ([]wkdb.Message, error) {
	rows, err := db.DB.SearchMessages(req)
	db.pause("search")
	return rows, err
}

func (db *historyBlockedMultiReadDB) LoadMsgsBatch(req []wkdb.BatchMsgRequest) ([]wkdb.BatchMsgResponse, error) {
	rows, err := db.DB.LoadMsgsBatch(req)
	db.pause("batch")
	return rows, err
}

func (db *historyBlockedMultiReadDB) GetUserLastMsgSeqBatch(uid string, channels []wkdb.Channel) (map[string]uint64, error) {
	rows, err := db.DB.GetUserLastMsgSeqBatch(uid, channels)
	db.pause("sender-batch")
	return rows, err
}

func TestHistoryMultiReadRechecksFenceAfterIO(t *testing.T) {
	for _, mode := range []string{"search", "batch", "sender-batch"} {
		t.Run(mode, func(t *testing.T) {
			base := historyTestDB(t, t.TempDir())
			defer base.Close()
			db := &historyBlockedMultiReadDB{DB: base, HistoryFenceStore: base.(wkdb.HistoryFenceStore), mode: mode, entered: make(chan struct{}), resume: make(chan struct{})}
			s := historyTestStore(db, &historyTestChannel{db: db})
			for i, ch := range []string{"a@c", "b@c"} {
				_, err := s.AppendMessages(context.Background(), ch, 1, []wkdb.Message{historyMessage(ch, int64(800+i))})
				historyMust(t, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			var workers sync.WaitGroup
			var resumeOnce sync.Once
			unblock := func() { resumeOnce.Do(func() { close(db.resume) }) }
			defer func() { unblock(); cancel(); workers.Wait() }()
			read := func() error {
				switch mode {
				case "search":
					rows, err := s.SearchMessages(wkdb.MessageSearchReq{Limit: 20})
					if err == nil && (len(rows) != 1 || rows[0].ChannelID != "b@c") {
						return errors.New("搜索返回了读取期间已冻结的来源")
					}
					return err
				case "batch":
					rows, err := s.LoadMsgsBatch([]wkdb.BatchMsgRequest{{ChannelId: "a@c", ChannelType: 1, MsgSeq: 1, Limit: 10}, {ChannelId: "b@c", ChannelType: 1, MsgSeq: 1, Limit: 10}})
					if err == nil && (len(rows) != 2 || len(rows[0].Messages) != 0 || len(rows[1].Messages) != 1) {
						return errors.New("批量读取返回了读取期间已冻结的来源")
					}
					return err
				default:
					rows, err := s.GetUserLastMsgSeqBatch("a", []wkdb.Channel{{ChannelId: "a@c", ChannelType: 1}, {ChannelId: "b@c", ChannelType: 1}})
					if err == nil && (len(rows) != 1 || rows["b@c:"+string([]byte{1})] != 1) {
						return errors.New("批量发送者水位返回了读取期间已冻结的来源")
					}
					return err
				}
			}
			readDone, frozen := make(chan error, 1), make(chan error, 1)
			workers.Add(1)
			go func() { defer workers.Done(); readDone <- read() }()
			<-db.entered
			workers.Add(1)
			go func() { defer workers.Done(); _, err := s.FreezeHistory(ctx, historyIdentity()); frozen <- err }()
			select {
			case err := <-frozen:
				historyMust(t, err)
			case <-time.After(time.Second):
				t.Fatal("多频道原始读取期间持锁阻塞了冻结")
			}
			unblock()
			historyMust(t, <-readDone)
		})
	}
}
