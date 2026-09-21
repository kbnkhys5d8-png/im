package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"golang.org/x/sys/unix"
)

func notifyStoreMessage(id int64, payload string) wkdb.Message {
	return wkdb.Message{RecvPacket: wkproto.RecvPacket{
		MessageID: id, MessageSeq: 2, ClientMsgNo: "failure-store-test",
		ChannelID: "test-channel", ChannelType: wkproto.ChannelTypeGroup,
		Payload: []byte(payload),
	}, Term: 3, SearchOutbox: true}
}

func openTestNotifyFailureStore(t *testing.T, dir string) *fileNotifyFailureStore {
	t.Helper()
	store, err := newNotifyFailureStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	return store.(*fileNotifyFailureStore)
}

func TestNotifyFailureStoreRoundTripAndRestart(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "failures")
	store := openTestNotifyFailureStore(t, dir)
	message := notifyStoreMessage(101, "保存完整消息")
	if err := store.Save([]wkdb.Message{message}, "retry_limit"); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		name string
		mode os.FileMode
	}{{dir, 0700}, {filepath.Join(dir, "101.json"), 0600}} {
		info, err := os.Stat(item.name)
		if err != nil || info.Mode().Perm() != item.mode {
			t.Fatalf("unexpected permissions for %s: info=%v err=%v", item.name, info, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "101.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record struct {
		Version int    `json:"version"`
		Reason  string `json:"reason"`
		Data    []byte `json:"data"`
	}
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	want, err := message.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if record.Version != 1 || record.Reason != "retry_limit" || !bytes.Equal(record.Data, want) {
		t.Fatal("saved envelope differs from the original message")
	}
	var decoded wkdb.Message
	if err := decoded.Unmarshal(record.Data); err != nil || decoded.MessageID != message.MessageID || !decoded.SearchOutbox {
		t.Fatalf("message round trip failed: %v", err)
	}
	// 重新构造实例，不依赖上一个实例的任何内存状态。
	reopened := openTestNotifyFailureStore(t, dir)
	if exists, err := reopened.Has(message.MessageID); err != nil || !exists {
		t.Fatalf("record missing after reopen: exists=%v err=%v", exists, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 || entries[0].Name() != "101.json" {
		t.Fatalf("unexpected temporary files: entries=%v err=%v", entries, err)
	}
}

func TestNotifyFailureStoreSameDataIsIdempotent(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "failures")
	store := openTestNotifyFailureStore(t, dir)
	message := notifyStoreMessage(102, "same")
	if err := store.Save([]wkdb.Message{message}, "first reason"); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "102.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save([]wkdb.Message{message, message}, "later reason"); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(filepath.Join(dir, "102.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("idempotent save overwrote the first record: %v", err)
	}
}

func TestNotifyFailureStoreConcurrentSameID(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "failures")
	stores := []*fileNotifyFailureStore{openTestNotifyFailureStore(t, dir), openTestNotifyFailureStore(t, dir)}
	message := notifyStoreMessage(103, "concurrent")
	results := make(chan error, 32)
	var calls sync.WaitGroup
	for i := 0; i < cap(results); i++ {
		calls.Add(1)
		go func(i int) {
			defer calls.Done()
			results <- stores[i%len(stores)].Save([]wkdb.Message{message}, "concurrent")
		}(i)
	}
	calls.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	if exists, err := stores[0].Has(message.MessageID); err != nil || !exists {
		t.Fatalf("concurrent record missing: exists=%v err=%v", exists, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("concurrent save left temporary files: entries=%v err=%v", entries, err)
	}
}

func TestNotifyFailureStoreConflictDoesNotOverwrite(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "failures")
	store := openTestNotifyFailureStore(t, dir)
	if err := store.Save([]wkdb.Message{notifyStoreMessage(104, "original")}, "first"); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(dir, "104.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save([]wkdb.Message{notifyStoreMessage(104, "different")}, "second"); !errors.Is(err, errNotifyFailureConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	after, err := os.ReadFile(filepath.Join(dir, "104.json"))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("conflicting save changed the original: %v", err)
	}
}

func TestNotifyFailureStoreConcurrentConflict(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "failures")
	stores := []*fileNotifyFailureStore{openTestNotifyFailureStore(t, dir), openTestNotifyFailureStore(t, dir)}
	messages := []wkdb.Message{notifyStoreMessage(111, "first"), notifyStoreMessage(111, "second")}
	type result struct {
		index int
		err   error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for i := range stores {
		go func(i int) {
			<-start
			results <- result{index: i, err: stores[i].Save([]wkdb.Message{messages[i]}, "conflict")}
		}(i)
	}
	close(start)
	winner, successes, conflicts := -1, 0, 0
	for range stores {
		result := <-results
		switch {
		case result.err == nil:
			winner, successes = result.index, successes+1
		case errors.Is(result.err, errNotifyFailureConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent save error: %v", result.err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent conflict results: successes=%d conflicts=%d", successes, conflicts)
	}
	file, err := os.ReadFile(filepath.Join(dir, "111.json"))
	if err != nil {
		t.Fatal(err)
	}
	var record notifyFailureRecord
	if err := json.Unmarshal(file, &record); err != nil || !bytes.Equal(record.Data, mustNotifyMessageData(t, messages[winner])) {
		t.Fatalf("losing writer changed the record: %v", err)
	}
}

func TestNotifyFailureStoreInvalidIDs(t *testing.T) {
	t.Parallel()
	store := openTestNotifyFailureStore(t, filepath.Join(t.TempDir(), "failures"))
	for _, test := range []struct {
		name string
		id   int64
	}{{"zero", 0}, {"negative", -1}} {
		t.Run(test.name, func(t *testing.T) {
			if err := store.Save([]wkdb.Message{notifyStoreMessage(test.id, "bad")}, "invalid"); err == nil {
				t.Fatal("Save accepted an invalid id")
			}
			if _, err := store.Has(test.id); err == nil {
				t.Fatal("Has accepted an invalid id")
			}
			if err := store.Remove([]int64{test.id}); err == nil {
				t.Fatal("Remove accepted an invalid id")
			}
		})
	}
}

func TestNotifyFailureStoreRejectsCorruptRecords(t *testing.T) {
	t.Parallel()
	message := notifyStoreMessage(105, "valid")
	data, err := message.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		data []byte
	}{
		{"broken_json", []byte("{")},
		{"unsupported_version", []byte(`{"version":2,"data":""}`)},
		{"invalid_base64", []byte(`{"version":1,"data":"#"}`)},
		{"empty_frame_decoder_panic", mustNotifyEnvelope(t, []byte{105, 0, 0, 0, 0, 0})},
		{"truncated_message", mustNotifyEnvelope(t, data[:len(data)-1])},
		{"trailing_message_bytes", mustNotifyEnvelope(t, append(append([]byte(nil), data...), 1))},
		{"wrong_message_id", mustNotifyEnvelope(t, mustNotifyMessageData(t, notifyStoreMessage(106, "wrong id")))},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "failures")
			store := openTestNotifyFailureStore(t, dir)
			name := filepath.Join(dir, "105.json")
			if err := os.WriteFile(name, test.data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := store.Has(105); err == nil {
				t.Fatal("Has treated a corrupt record as valid")
			}
			if err := store.Save([]wkdb.Message{message}, "retry"); err == nil {
				t.Fatal("Save overwrote or accepted a corrupt record")
			}
			after, err := os.ReadFile(name)
			if err != nil || !bytes.Equal(after, test.data) {
				t.Fatalf("corrupt original was changed: %v", err)
			}
		})
	}
}

func mustNotifyMessageData(t *testing.T, message wkdb.Message) []byte {
	t.Helper()
	data, err := message.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func mustNotifyEnvelope(t *testing.T, data []byte) []byte {
	t.Helper()
	envelope, err := json.Marshal(notifyFailureRecord{Version: 1, Reason: "test", Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func TestNotifyFailureStoreRejectsUnsafePaths(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	realDir := filepath.Join(parent, "real")
	if err := os.Mkdir(realDir, 0700); err != nil {
		t.Fatal(err)
	}
	linkDir := filepath.Join(parent, "link")
	if err := os.Symlink(realDir, linkDir); err != nil {
		t.Fatal(err)
	}
	if _, err := newNotifyFailureStore(linkDir); err == nil {
		t.Fatal("constructor followed a directory symlink")
	}
	if _, err := newNotifyFailureStore(filepath.Join(parent, "missing", "failures")); err == nil {
		t.Fatal("constructor silently created an unsynced parent hierarchy")
	}
	store := openTestNotifyFailureStore(t, realDir)
	outside := filepath.Join(parent, "outside")
	if err := os.WriteFile(outside, []byte("must remain untouched"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(realDir, "107.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Has(107); err == nil {
		t.Fatal("Has followed a record symlink")
	}
	if err := store.Save([]wkdb.Message{notifyStoreMessage(107, "new")}, "retry"); err == nil {
		t.Fatal("Save accepted a record symlink")
	}
	if err := store.Remove([]int64{107}); err == nil {
		t.Fatal("Remove accepted a record symlink")
	}
	contents, err := os.ReadFile(outside)
	if err != nil || string(contents) != "must remain untouched" {
		t.Fatalf("outside file changed: %v", err)
	}
}

func TestNotifyFailureStoreRejectsFIFOWithoutBlocking(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "failures")
	store := openTestNotifyFailureStore(t, dir)
	name := filepath.Join(dir, "116.json")
	if err := unix.Mkfifo(name, 0600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		run  func() error
	}{
		{"has", func() error { _, err := store.Has(116); return err }},
		{"save", func() error { return store.Save([]wkdb.Message{notifyStoreMessage(116, "new")}, "failure") }},
		{"remove", func() error { return store.Remove([]int64{116}) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			result := make(chan error, 1)
			go func() { result <- test.run() }()
			select {
			case err := <-result:
				if err == nil {
					t.Fatal("operation accepted a FIFO record")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("operation blocked opening a FIFO record")
			}
		})
	}
	info, err := os.Lstat(name)
	if err != nil || info.Mode()&os.ModeNamedPipe == 0 {
		t.Fatalf("FIFO was modified or removed: %v", err)
	}
}

func TestNotifyFailureStoreSyncFailures(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		failDir      bool
		wantExisting bool
	}{{"file_sync", false, false}, {"directory_sync", true, true}} {
		t.Run(test.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "failures")
			store := openTestNotifyFailureStore(t, dir)
			injected := errors.New("injected sync failure")
			store.syncFile = func(file *os.File) error {
				info, err := file.Stat()
				if err != nil {
					return err
				}
				if info.IsDir() == test.failDir {
					return injected
				}
				return file.Sync()
			}
			message := notifyStoreMessage(108, "durable")
			if err := store.Save([]wkdb.Message{message}, "sync failure"); !errors.Is(err, injected) {
				t.Fatalf("Save hid sync failure: %v", err)
			}
			_, err := os.Stat(filepath.Join(dir, "108.json"))
			if test.wantExisting && err != nil || !test.wantExisting && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("unexpected publication state: %v", err)
			}
			store.syncFile = (*os.File).Sync
			// 发布后的目录同步失败允许幂等重试，不能删除已发布的原记录。
			if err := store.Save([]wkdb.Message{message}, "retry"); err != nil {
				t.Fatal(err)
			}
			if exists, err := store.Has(108); err != nil || !exists {
				t.Fatalf("retry did not recover: exists=%v err=%v", exists, err)
			}
			entries, err := os.ReadDir(dir)
			if err != nil || len(entries) != 1 {
				t.Fatalf("failed save left temporary files: entries=%v err=%v", entries, err)
			}
		})
	}
}

func TestNotifyFailureStoreHasFailsClosed(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "failures")
	store := openTestNotifyFailureStore(t, dir)
	if err := store.Save([]wkdb.Message{notifyStoreMessage(112, "saved")}, "failure"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		failDir bool
	}{{"file_sync", false}, {"directory_sync", true}} {
		t.Run(test.name, func(t *testing.T) {
			injected := errors.New("injected Has sync failure")
			store.syncFile = func(file *os.File) error {
				info, err := file.Stat()
				if err != nil {
					return err
				}
				if info.IsDir() == test.failDir {
					return injected
				}
				return file.Sync()
			}
			if exists, err := store.Has(112); exists || !errors.Is(err, injected) {
				t.Fatalf("Has hid sync failure: exists=%v err=%v", exists, err)
			}
		})
	}
	store.syncFile = (*os.File).Sync
	if err := os.Chmod(filepath.Join(dir, "112.json"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Has(112); err == nil {
		t.Fatal("Has accepted a nonprivate record")
	}
	if err := os.Rename(dir, dir+"-moved"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Has(112); err == nil {
		t.Fatal("Has treated a missing store directory as a missing message")
	}
	if err := store.Save([]wkdb.Message{notifyStoreMessage(113, "new")}, "failure"); err == nil {
		t.Fatal("Save silently recreated a missing store directory")
	}
}

func TestNotifyFailureStoreRemoveAndSyncRetry(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "failures")
	store := openTestNotifyFailureStore(t, dir)
	if err := store.Save([]wkdb.Message{notifyStoreMessage(109, "delivered"), notifyStoreMessage(110, "keep")}, "failure"); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected remove sync failure")
	store.syncFile = func(*os.File) error { return injected }
	if err := store.Remove([]int64{109}); !errors.Is(err, injected) {
		t.Fatalf("Remove hid sync failure: %v", err)
	}
	store.syncFile = (*os.File).Sync
	if err := store.Remove([]int64{109, 109, 999}); err != nil {
		t.Fatalf("idempotent Remove retry failed: %v", err)
	}
	if exists, err := store.Has(109); err != nil || exists {
		t.Fatalf("delivered record remains: exists=%v err=%v", exists, err)
	}
	if exists, err := store.Has(110); err != nil || !exists {
		t.Fatalf("Remove affected another record: exists=%v err=%v", exists, err)
	}
}

func TestNotifyFailureStoreRemovePartialFailureSyncs(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "failures")
	store := openTestNotifyFailureStore(t, dir)
	if err := store.Save([]wkdb.Message{notifyStoreMessage(114, "delivered")}, "failure"); err != nil {
		t.Fatal(err)
	}
	badTarget := filepath.Join(dir, "115.json")
	if err := os.Mkdir(badTarget, 0700); err != nil {
		t.Fatal(err)
	}
	syncCalls := 0
	store.syncFile = func(file *os.File) error {
		syncCalls++
		return file.Sync()
	}
	if err := store.Remove([]int64{114, 115}); err == nil {
		t.Fatal("Remove accepted a directory target")
	}
	if syncCalls != 1 {
		t.Fatalf("partial Remove did not sync the directory: calls=%d", syncCalls)
	}
	if _, err := os.Stat(filepath.Join(dir, "114.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("first record was not removed: %v", err)
	}
	if info, err := os.Stat(badTarget); err != nil || !info.IsDir() {
		t.Fatalf("invalid removal target was changed: %v", err)
	}
}
