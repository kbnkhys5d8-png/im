package wkdb

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

var (
	ErrSearchOutboxEpochRequired = errors.New("search outbox task epoch is required")
	ErrSearchOutboxStaleEpoch    = errors.New("search outbox task epoch is stale")
	ErrSearchOutboxState         = errors.New("search outbox task state does not allow this operation")
)

// SearchOutboxTask 用完整身份和持久代次约束每次修改，旧请求不能操作恢复后的任务。
type SearchOutboxTask struct {
	Identity SearchOutboxIdentity `json:"identity"`
	Epoch    uint64               `json:"epoch"`
}

// SearchOutboxTaskStore 是第二版协议专用接口，旧 ACK 不具备代次凭据。
type SearchOutboxTaskStore interface {
	AckSearchOutboxTasks([]SearchOutboxTask) error
	QuarantineSearchOutbox(SearchOutboxTask, string) error
	RestoreSearchOutbox(SearchOutboxTask) (uint64, error)
	ListSearchOutboxQuarantine(int) ([]SearchOutboxQuarantineRecord, error)
}

var _ SearchOutboxTaskStore = (*wukongDB)(nil)

// SearchOutboxQuarantineRecord 只供受限管理读取；原始数据不得写入日志或普通错误响应。
type SearchOutboxQuarantineRecord struct {
	ShardID       uint32                `json:"shard_id"`
	Identity      *SearchOutboxIdentity `json:"identity,omitempty"`
	RawKey        []byte                `json:"raw_key"`
	RawValue      []byte                `json:"raw_value"`
	Epoch         uint64                `json:"epoch"`
	Reason        string                `json:"reason"`
	QuarantinedAt int64                 `json:"quarantined_at"`
}

const (
	searchOutboxActive       byte = 1
	searchOutboxQuarantined  byte = 2
	searchOutboxAcknowledged byte = 3
)

type searchOutboxState struct {
	Epoch  uint64
	Status byte
}

func (state searchOutboxState) marshal() []byte {
	value := make([]byte, 10)
	value[0] = 1
	value[1] = state.Status
	binary.BigEndian.PutUint64(value[2:], state.Epoch)
	return value
}

func (task SearchOutboxTask) key() ([]byte, error) {
	if err := task.Identity.Validate(); err != nil {
		return nil, err
	}
	if task.Epoch == 0 {
		return nil, ErrSearchOutboxEpochRequired
	}
	return key.NewSearchOutboxKey(task.Identity.ChannelID, task.Identity.ChannelType, task.Identity.MessageSeq, task.Identity.MessageID)
}

// 隔离原因只接受固定代号，拒绝存入可能带有聊天正文或凭据的原始错误。
func ValidSearchOutboxQuarantineReason(reason string) bool {
	switch reason {
	case "invalid_record", "invalid_timestamp", "source_oversize", "invalid_projection", "projection_oversize", "bulk_item_rejected", "term_too_long":
		return true
	default:
		return false
	}
}

func (wk *wukongDB) loadSearchOutboxBytes(shardID uint32, rawKey []byte) ([]byte, error) {
	value, closer, err := wk.shardDBById(shardID).Get(rawKey)
	if err != nil {
		return nil, err
	}
	copied := append([]byte(nil), value...)
	if err := closer.Close(); err != nil {
		return nil, err
	}
	return copied, nil
}

func (wk *wukongDB) loadSearchOutboxState(shardID uint32, rawKey []byte) (searchOutboxState, bool, error) {
	value, err := wk.loadSearchOutboxBytes(shardID, key.NewSearchOutboxStateKey(rawKey))
	if errors.Is(err, pebble.ErrNotFound) {
		// 升级前已存在的任务从第一代开始，返回给插件前再同步落盘。
		return searchOutboxState{Epoch: 1, Status: searchOutboxActive}, false, nil
	}
	if err != nil {
		return searchOutboxState{}, false, fmt.Errorf("load search outbox state: %w", err)
	}
	if len(value) != 10 || value[0] != 1 || value[1] < searchOutboxActive || value[1] > searchOutboxAcknowledged || binary.BigEndian.Uint64(value[2:]) == 0 {
		return searchOutboxState{}, false, errors.New("search outbox durable state is corrupt")
	}
	return searchOutboxState{Epoch: binary.BigEndian.Uint64(value[2:]), Status: value[1]}, true, nil
}

func (wk *wukongDB) AckSearchOutboxTasks(tasks []SearchOutboxTask) error {
	if len(tasks) == 0 {
		return nil
	}
	if len(tasks) > MaxSearchOutboxPullLimit {
		return errors.New("search outbox ack task count exceeds limit")
	}
	// 请求格式一次预检，状态则在各分片自己的锁内校验，绝不同时持有多个分片锁。
	byShard := make(map[uint32][]SearchOutboxTask)
	shards := make([]uint32, 0)
	epochs := make(map[SearchOutboxIdentity]uint64, len(tasks))
	for _, task := range tasks {
		if _, err := task.key(); err != nil {
			return err
		}
		if epoch, ok := epochs[task.Identity]; ok {
			if epoch != task.Epoch {
				return ErrSearchOutboxStaleEpoch
			}
			continue
		}
		epochs[task.Identity] = task.Epoch
		shardID := wk.GetChannelShardIndex(task.Identity.ChannelID, task.Identity.ChannelType)
		if _, ok := byShard[shardID]; !ok {
			shards = append(shards, shardID)
		}
		byShard[shardID] = append(byShard[shardID], task)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	for _, shardID := range shards {
		if err := wk.ackSearchOutboxShard(shardID, byShard[shardID]); err != nil {
			return err
		}
	}
	return nil
}

func (wk *wukongDB) ackSearchOutboxShard(shardID uint32, tasks []SearchOutboxTask) error {
	lock := &wk.searchOutboxMu[shardID]
	lock.Lock()
	defer lock.Unlock()
	var batch *Batch
	for _, task := range tasks {
		rawKey, err := task.key()
		if err != nil {
			return err
		}
		state, _, err := wk.loadSearchOutboxState(shardID, rawKey)
		if err != nil {
			return err
		}
		if state.Epoch != task.Epoch {
			return ErrSearchOutboxStaleEpoch
		}
		if state.Status == searchOutboxAcknowledged {
			continue
		}
		if state.Status != searchOutboxActive {
			return ErrSearchOutboxState
		}
		value, err := wk.loadSearchOutboxBytes(shardID, rawKey)
		if errors.Is(err, pebble.ErrNotFound) {
			return ErrSearchOutboxState
		}
		if err != nil {
			return fmt.Errorf("load search outbox ack record: %w", err)
		}
		if _, err := decodeSearchOutboxRecord(rawKey, value); err != nil {
			return err
		}
		if batch == nil {
			batch = wk.shardBatchDBById(shardID).NewBatch()
		}
		state.Status = searchOutboxAcknowledged
		batch.Set(key.NewSearchOutboxStateKey(rawKey), state.marshal())
		batch.Delete(rawKey)
	}
	if batch != nil {
		if err := batch.CommitWait(); err != nil {
			return fmt.Errorf("commit search outbox ack: %w", err)
		}
	}
	return nil
}

func (wk *wukongDB) QuarantineSearchOutbox(task SearchOutboxTask, reason string) error {
	if !ValidSearchOutboxQuarantineReason(reason) {
		return errors.New("invalid search outbox quarantine reason")
	}
	rawKey, err := task.key()
	if err != nil {
		return err
	}
	shardID := wk.GetChannelShardIndex(task.Identity.ChannelID, task.Identity.ChannelType)
	lock := &wk.searchOutboxMu[shardID]
	lock.Lock()
	defer lock.Unlock()
	state, _, err := wk.loadSearchOutboxState(shardID, rawKey)
	if err != nil {
		return err
	}
	if state.Epoch != task.Epoch {
		return ErrSearchOutboxStaleEpoch
	}
	if state.Status == searchOutboxQuarantined {
		_, err := wk.loadSearchOutboxQuarantine(shardID, rawKey, state.Epoch)
		return err
	}
	if state.Status != searchOutboxActive {
		return ErrSearchOutboxState
	}
	value, err := wk.loadSearchOutboxBytes(shardID, rawKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return ErrSearchOutboxState
	}
	if err != nil {
		return fmt.Errorf("load search outbox quarantine source: %w", err)
	}
	return wk.quarantineSearchOutboxRawLocked(shardID, rawKey, value, state, reason)
}

func (wk *wukongDB) quarantineSearchOutboxRawLocked(shardID uint32, rawKey, value []byte, state searchOutboxState, reason string) error {
	if state.Status != searchOutboxActive {
		return ErrSearchOutboxState
	}
	record := SearchOutboxQuarantineRecord{
		ShardID: shardID, RawKey: append([]byte(nil), rawKey...), RawValue: append([]byte(nil), value...),
		Epoch: state.Epoch, Reason: reason, QuarantinedAt: time.Now().Unix(),
	}
	channelID, channelType, sequence, messageID, err := key.ParseSearchOutboxKey(rawKey)
	if err == nil {
		record.Identity = &SearchOutboxIdentity{ChannelID: channelID, ChannelType: channelType, MessageSeq: sequence, MessageID: messageID}
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return errors.New("encode search outbox quarantine record")
	}
	state.Status = searchOutboxQuarantined
	batch := wk.shardBatchDBById(shardID).NewBatch()
	batch.Set(key.NewSearchOutboxQuarantineKey(rawKey), encoded)
	batch.Set(key.NewSearchOutboxStateKey(rawKey), state.marshal())
	batch.Delete(rawKey)
	// 只有同分片同步提交成功，才能让源任务退出正常队列。
	if err := batch.CommitWait(); err != nil {
		return fmt.Errorf("commit search outbox quarantine: %w", err)
	}
	return nil
}

func (wk *wukongDB) loadSearchOutboxQuarantine(shardID uint32, rawKey []byte, epoch uint64) (SearchOutboxQuarantineRecord, error) {
	value, err := wk.loadSearchOutboxBytes(shardID, key.NewSearchOutboxQuarantineKey(rawKey))
	if err != nil {
		return SearchOutboxQuarantineRecord{}, fmt.Errorf("load search outbox quarantine: %w", err)
	}
	var record SearchOutboxQuarantineRecord
	if json.Unmarshal(value, &record) != nil || record.ShardID != shardID || record.Epoch != epoch || !bytes.Equal(record.RawKey, rawKey) || !ValidSearchOutboxQuarantineReason(record.Reason) || record.QuarantinedAt <= 0 {
		return SearchOutboxQuarantineRecord{}, errors.New("search outbox quarantine record is corrupt")
	}
	return record, nil
}

func (wk *wukongDB) RestoreSearchOutbox(task SearchOutboxTask) (uint64, error) {
	rawKey, err := task.key()
	if err != nil {
		return 0, err
	}
	shardID := wk.GetChannelShardIndex(task.Identity.ChannelID, task.Identity.ChannelType)
	lock := &wk.searchOutboxMu[shardID]
	lock.Lock()
	defer lock.Unlock()
	state, exists, err := wk.loadSearchOutboxState(shardID, rawKey)
	if err != nil {
		return 0, err
	}
	if task.Epoch < math.MaxUint64 && state.Epoch == task.Epoch+1 {
		// 响应丢失后的重试只回报已完成的恢复，不再次恢复已失败的新代任务。
		return state.Epoch, nil
	}
	if state.Epoch != task.Epoch {
		return 0, ErrSearchOutboxStaleEpoch
	}
	if !exists || state.Status != searchOutboxQuarantined || state.Epoch == math.MaxUint64 {
		return 0, ErrSearchOutboxState
	}
	record, err := wk.loadSearchOutboxQuarantine(shardID, rawKey, task.Epoch)
	if err != nil {
		return 0, err
	}
	decoded, err := decodeSearchOutboxRecord(rawKey, record.RawValue)
	if err != nil || decoded.Message.Timestamp <= 0 {
		return 0, ErrSearchOutboxState
	}
	// 显式恢复也不能复活已被 Raft 截断或替换的聊天消息。
	current, err := wk.LoadMsg(task.Identity.ChannelID, task.Identity.ChannelType, task.Identity.MessageSeq)
	if err != nil {
		return 0, fmt.Errorf("check search outbox restore source: %w", err)
	}
	if searchOutboxIdentityFromMessage(current) != task.Identity {
		return 0, ErrSearchOutboxState
	}
	if _, err := wk.loadSearchOutboxBytes(shardID, rawKey); !errors.Is(err, pebble.ErrNotFound) {
		if err != nil {
			return 0, err
		}
		return 0, ErrSearchOutboxState
	}
	state.Epoch++
	state.Status = searchOutboxActive
	batch := wk.shardBatchDBById(shardID).NewBatch()
	batch.Set(rawKey, record.RawValue)
	batch.Set(key.NewSearchOutboxStateKey(rawKey), state.marshal())
	batch.Delete(key.NewSearchOutboxQuarantineKey(rawKey))
	if err := batch.CommitWait(); err != nil {
		return 0, fmt.Errorf("commit search outbox restore: %w", err)
	}
	return state.Epoch, nil
}

func (wk *wukongDB) ListSearchOutboxQuarantine(limit int) ([]SearchOutboxQuarantineRecord, error) {
	if limit < 1 || limit > MaxSearchOutboxPullLimit {
		return nil, errors.New("invalid search outbox quarantine list limit")
	}
	// 列表允许观察到并发状态转换前的快照；实际修改仍必须核对持久代次。
	records := make([]SearchOutboxQuarantineRecord, 0, limit)
	for shardID := uint32(0); shardID < wk.shardNum; shardID++ {
		iter := wk.shardDBById(shardID).NewIter(&pebble.IterOptions{LowerBound: key.NewSearchOutboxQuarantineLowKey(), UpperBound: key.NewSearchOutboxQuarantineHighKey()})
		for iter.First(); iter.Valid() && len(records) < limit; iter.Next() {
			var record SearchOutboxQuarantineRecord
			if json.Unmarshal(iter.Value(), &record) != nil || record.ShardID != shardID || record.Epoch == 0 || !bytes.Equal(iter.Key(), key.NewSearchOutboxQuarantineKey(record.RawKey)) || !ValidSearchOutboxQuarantineReason(record.Reason) {
				iter.Close()
				return nil, errors.New("search outbox quarantine record is corrupt")
			}
			records = append(records, record)
		}
		iterErr := iter.Error()
		closeErr := iter.Close()
		if iterErr != nil {
			return nil, fmt.Errorf("iterate search outbox quarantine: %w", iterErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
		if len(records) == limit {
			break
		}
	}
	return records, nil
}

// 只扫描隔离键计数，不读取或解码聊天正文；结果采用各分片读取时的快照口径。
func (wk *wukongDB) countSearchOutboxQuarantine() (uint64, error) {
	var count uint64
	for shardID := uint32(0); shardID < wk.shardNum; shardID++ {
		iter := wk.shardDBById(shardID).NewIter(&pebble.IterOptions{LowerBound: key.NewSearchOutboxQuarantineLowKey(), UpperBound: key.NewSearchOutboxQuarantineHighKey()})
		for iter.First(); iter.Valid(); iter.Next() {
			count++
		}
		iterErr := iter.Error()
		closeErr := iter.Close()
		if iterErr != nil {
			return 0, fmt.Errorf("count search outbox quarantine: %w", iterErr)
		}
		if closeErr != nil {
			return 0, closeErr
		}
	}
	return count, nil
}

// 长扫描不等待忙分片；任务保留到下一轮，其他分片可继续返回记录。
func (wk *wukongDB) pinSearchOutboxRecord(shardID uint32, rawKey, expected []byte) (uint64, bool, error) {
	lock := &wk.searchOutboxMu[shardID]
	if !lock.TryLock() {
		return 0, false, nil
	}
	defer lock.Unlock()
	current, err := wk.loadSearchOutboxBytes(shardID, rawKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if !bytes.Equal(current, expected) {
		return 0, false, nil
	}
	state, exists, err := wk.loadSearchOutboxState(shardID, rawKey)
	if err != nil {
		return 0, false, err
	}
	if state.Status != searchOutboxActive {
		return 0, false, ErrSearchOutboxState
	}
	if !exists {
		batch := wk.shardBatchDBById(shardID).NewBatch()
		batch.Set(key.NewSearchOutboxStateKey(rawKey), state.marshal())
		if err := batch.CommitWait(); err != nil {
			return 0, false, fmt.Errorf("persist search outbox epoch: %w", err)
		}
	}
	return state.Epoch, true, nil
}

// 忙分片留待下轮；隔离前重新读取源记录，禁止用旧快照删除新值。
func (wk *wukongDB) quarantineSearchOutboxCandidate(shardID uint32, rawKey, expected []byte, reason string) (bool, error) {
	lock := &wk.searchOutboxMu[shardID]
	if !lock.TryLock() {
		return false, nil
	}
	defer lock.Unlock()
	current, err := wk.loadSearchOutboxBytes(shardID, rawKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if !bytes.Equal(current, expected) {
		return false, nil
	}
	state, _, err := wk.loadSearchOutboxState(shardID, rawKey)
	if err != nil {
		return false, err
	}
	if err := wk.quarantineSearchOutboxRawLocked(shardID, rawKey, current, state, reason); err != nil {
		return false, err
	}
	return true, nil
}
