package webhook

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"golang.org/x/sys/unix"
)

type notifyFailureStore interface {
	Save([]wkdb.Message, string) error
	Has(int64) (bool, error)
	Remove([]int64) error
}

type notifyFailureRecord struct {
	Version int    `json:"version"`
	Reason  string `json:"reason"`
	Data    []byte `json:"data"` // JSON 自动使用 base64 保存原始消息二进制。
}

var errNotifyFailureConflict = errors.New("notify failure message data conflict")

type fileNotifyFailureStore struct {
	dir       string
	directory os.FileInfo
	mu        sync.Mutex
	// 通过实例注入同步失败以验证错误路径，生产始终使用 File.Sync。
	syncFile func(*os.File) error
}

func newNotifyFailureStore(dir string) (store notifyFailureStore, retErr error) {
	if dir == "" {
		return nil, errors.New("notify failure directory is empty")
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve notify failure directory: %w", err)
	}
	if filepath.Dir(dir) == dir {
		return nil, errors.New("notify failure directory cannot be a filesystem root")
	}
	// 调用方先创建 DataDir；只创建最后一级，避免遗漏多级新目录的父目录同步。
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create notify failure directory: %w", err)
	}
	directory, err := openNotifyFailureDirectory(dir)
	if err != nil {
		return nil, err
	}
	defer func() { retErr = errors.Join(retErr, directory.Close()) }()
	if err := directory.Chmod(0700); err != nil {
		return nil, fmt.Errorf("restrict notify failure directory: %w", err)
	}
	info, err := directory.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat notify failure directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		return nil, fmt.Errorf("sync notify failure directory: %w", err)
	}
	parent, err := os.Open(filepath.Dir(dir))
	if err != nil {
		return nil, fmt.Errorf("open notify failure parent: %w", err)
	}
	if err := errors.Join(parent.Sync(), parent.Close()); err != nil {
		return nil, fmt.Errorf("sync notify failure parent: %w", err)
	}
	return &fileNotifyFailureStore{dir: dir, directory: info, syncFile: (*os.File).Sync}, nil
}

func openNotifyFailureDirectory(dir string) (*os.File, error) {
	fd, err := unix.Open(dir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open notify failure directory: %w", err)
	}
	return os.NewFile(uintptr(fd), dir), nil
}

func (s *fileNotifyFailureStore) openDirectory() (*os.File, error) {
	directory, err := openNotifyFailureDirectory(s.dir)
	if err != nil {
		return nil, err
	}
	info, err := directory.Stat()
	if err == nil && (!os.SameFile(info, s.directory) || info.Mode().Perm() != 0700) {
		err = errors.New("notify failure directory identity or permissions changed")
	}
	if err != nil {
		return nil, errors.Join(err, directory.Close())
	}
	return directory, nil
}

func (s *fileNotifyFailureStore) filename(id int64) string {
	return filepath.Join(s.dir, strconv.FormatInt(id, 10)+".json")
}

func (s *fileNotifyFailureStore) Save(messages []wkdb.Message, reason string) (retErr error) {
	for _, message := range messages {
		if message.MessageID <= 0 {
			return errors.New("notify failure message id must be positive")
		}
	}
	if len(messages) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	directory, err := s.openDirectory()
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, directory.Close()) }()
	// 每条独立持久化；批量中途出错时，已保存记录保留，调用方可以幂等重试。
	for _, message := range messages {
		if err := s.saveOne(directory, message, reason); err != nil {
			return fmt.Errorf("save notify failure %d: %w", message.MessageID, err)
		}
	}
	return nil
}

func (s *fileNotifyFailureStore) saveOne(directory *os.File, message wkdb.Message, reason string) (retErr error) {
	data, err := message.Marshal()
	if err != nil {
		return fmt.Errorf("marshal notify failure message: %w", err)
	}
	envelope, err := json.Marshal(notifyFailureRecord{Version: 1, Reason: reason, Data: data})
	if err != nil {
		return fmt.Errorf("marshal notify failure envelope: %w", err)
	}
	file, err := os.CreateTemp(s.dir, ".notify-failure-*")
	if err != nil {
		return fmt.Errorf("create notify failure temporary file: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			retErr = errors.Join(retErr, file.Close())
		}
		if err := os.Remove(file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(retErr, fmt.Errorf("remove notify failure temporary file: %w", err))
		}
		// 同时持久化无覆盖发布和临时文件移除；发布后同步失败不得回滚原记录。
		if err := s.syncFile(directory); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("sync notify failure directory: %w", err))
		}
	}()
	if err := file.Chmod(0600); err != nil {
		return fmt.Errorf("restrict notify failure file: %w", err)
	}
	if n, err := file.Write(envelope); err != nil {
		return fmt.Errorf("write notify failure file: %w", err)
	} else if n != len(envelope) {
		return fmt.Errorf("write notify failure file: %w", io.ErrShortWrite)
	}
	if err := s.syncFile(file); err != nil {
		return fmt.Errorf("sync notify failure file: %w", err)
	}
	err = file.Close()
	closed = true
	if err != nil {
		return fmt.Errorf("close notify failure file: %w", err)
	}
	// Link 在目标已存在时失败，跨实例并发也不会覆盖此前完整记录。
	if err := os.Link(file.Name(), s.filename(message.MessageID)); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("publish notify failure file: %w", err)
		}
		existing, err := s.readRecord(message.MessageID)
		if err != nil {
			return err
		}
		if !bytes.Equal(existing.Data, data) {
			return errNotifyFailureConflict
		}
		// 原因可能由首次失败变成重试耗尽；相同消息保留首次落盘的原因。
	}
	return nil
}

func (s *fileNotifyFailureStore) readRecord(id int64) (record notifyFailureRecord, retErr error) {
	name := s.filename(id)
	// O_NOFOLLOW 拒绝符号链接，O_NONBLOCK 防止误把 FIFO 打开后无限等待。
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return record, fmt.Errorf("open notify failure record: %w", err)
	}
	file := os.NewFile(uintptr(fd), name)
	defer func() { retErr = errors.Join(retErr, file.Close()) }()
	info, err := file.Stat()
	if err != nil {
		return record, fmt.Errorf("stat notify failure record: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return record, errors.New("notify failure record is not a private regular file")
	}
	data, err := io.ReadAll(file)
	if err != nil {
		return record, fmt.Errorf("read notify failure record: %w", err)
	}
	if err := json.Unmarshal(data, &record); err != nil {
		return record, fmt.Errorf("decode notify failure envelope: %w", err)
	}
	if record.Version != 1 {
		return record, errors.New("unsupported notify failure envelope version")
	}
	if err := verifyNotifyFailureMessage(record.Data, id); err != nil {
		return record, err
	}
	if err := s.syncFile(file); err != nil {
		return record, fmt.Errorf("sync existing notify failure record: %w", err)
	}
	return record, nil
}

func verifyNotifyFailureMessage(data []byte, id int64) (retErr error) {
	// 旧协议解码器对损坏的空帧可能 panic；只在磁盘数据验证边界转为错误，禁止放行补发。
	defer func() {
		if recover() != nil {
			retErr = errors.New("notify failure message decoder panicked on corrupt data")
		}
	}()
	var message wkdb.Message
	if err := message.Unmarshal(data); err != nil {
		return fmt.Errorf("decode notify failure message: %w", err)
	}
	canonical, err := message.Marshal()
	if err != nil {
		return fmt.Errorf("verify notify failure message: %w", err)
	}
	if message.MessageID != id || !bytes.Equal(canonical, data) {
		return errors.New("notify failure message id or encoding mismatch")
	}
	return nil
}

func (s *fileNotifyFailureStore) Has(id int64) (exists bool, retErr error) {
	if id <= 0 {
		return false, errors.New("notify failure message id must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	directory, err := s.openDirectory()
	if err != nil {
		return false, err
	}
	defer func() { retErr = errors.Join(retErr, directory.Close()) }()
	if _, err := s.readRecord(id); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	// 可能读到另一实例刚发布的记录；返回 true 前确认文件及目录均已同步。
	if err := s.syncFile(directory); err != nil {
		return false, fmt.Errorf("sync queried notify failure directory: %w", err)
	}
	return true, nil
}

func (s *fileNotifyFailureStore) Remove(ids []int64) (retErr error) {
	for _, id := range ids {
		if id <= 0 {
			return errors.New("notify failure message id must be positive")
		}
	}
	if len(ids) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	directory, err := s.openDirectory()
	if err != nil {
		return err
	}
	defer func() {
		// 即使中途失败或记录已不存在，也同步此前可能完成的删除，便于安全重试。
		if err := s.syncFile(directory); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("sync removed notify failure records: %w", err))
		}
		retErr = errors.Join(retErr, directory.Close())
	}()
	for _, id := range ids {
		name := s.filename(id)
		info, err := os.Lstat(name)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("stat notify failure record for removal: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
			return errors.New("notify failure removal target is not a private regular file")
		}
		if err := os.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove notify failure record: %w", err)
		}
	}
	return nil
}
