package wkdb

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
)

var (
	ErrHistoryFenced            = errors.New("source history is fenced")
	ErrHistoryConflict          = errors.New("history fence identity conflict")
	ErrHistoryUnsupported       = errors.New("history migration requires a single local leader")
	ErrHistoryChannelMissing    = errors.New("history source channel config is missing")
	ErrHistoryStreamUnsupported = errors.New("stream history is unsupported")
)

type HistoryFence struct {
	TaskID        string `json:"task_id"`
	SourceUID     string `json:"source_uid"`
	PeerUID       string `json:"peer_uid"`
	ChannelID     string `json:"source_channel"`
	HighWatermark uint64 `json:"high_watermark"`
}

// 独立能力接口保留旧 DB/fakeDB 的兼容性，正式存储必须实现此能力。
type HistoryFenceStore interface {
	GetHistoryFence(string, uint8) (*HistoryFence, error)
	GetReleasedHistoryFence(string, string) (*HistoryFence, error)
	FreezeHistoryFence(HistoryFence) (HistoryFence, error)
	FreezeEmptyHistoryFence(HistoryFence) (HistoryFence, error)
	SetHistoryFence(HistoryFence) error
	ReleaseHistoryFence(HistoryFence) error
	LoadHistoryMessages(string, uint64, uint64, int) ([]Message, error)
}

// 专用导出读取补齐原始 header；不改变现有普通范围迭代器的兼容行为。
func (wk *wukongDB) LoadHistoryMessages(channel string, start, end uint64, limit int) ([]Message, error) {
	rows, err := wk.LoadNextRangeMsgs(channel, 1, start, end, limit)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		if err := wk.restoreHistoryHeader(&rows[i]); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func (wk *wukongDB) restoreHistoryHeader(message *Message) error {
	value, closer, err := wk.channelDb(message.ChannelID, message.ChannelType).Get(key.NewMessageColumnKey(message.ChannelID, message.ChannelType, uint64(message.MessageSeq), key.TableMessage.Column.Header))
	if err != nil {
		return err
	}
	defer closer.Close()
	if len(value) != 1 {
		return ErrHistoryConflict
	}
	message.Framer = wkproto.FramerFromUint8(value[0])
	return nil
}

var _ HistoryFenceStore = (*wukongDB)(nil)

func validHistoryFence(f HistoryFence) bool {
	decoded, err := hex.DecodeString(f.TaskID)
	if err != nil || len(decoded) != 32 || f.TaskID != strings.ToLower(f.TaskID) || f.SourceUID == "" || f.PeerUID == "" ||
		strings.TrimSpace(f.SourceUID) != f.SourceUID || strings.TrimSpace(f.PeerUID) != f.PeerUID || !utf8.ValidString(f.SourceUID+f.PeerUID) ||
		strings.Contains(f.SourceUID+f.PeerUID, "@") || f.HighWatermark > math.MaxUint32 {
		return false
	}
	left, right := wkutil.HashCrc32(f.SourceUID), wkutil.HashCrc32(f.PeerUID)
	if left == right {
		return false
	}
	channel := f.PeerUID + "@" + f.SourceUID
	if left > right {
		channel = f.SourceUID + "@" + f.PeerUID
	}
	return f.ChannelID == channel
}

func (wk *wukongDB) GetReleasedHistoryFence(channel, task string) (*HistoryFence, error) {
	wk.historyFenceMu.RLock()
	defer wk.historyFenceMu.RUnlock()
	return wk.readHistoryFence(channel, historyReleasedKey(channel, task))
}

func historyReleasedKey(channel, task string) []byte {
	return append(historyFenceKey(channel, 2), []byte("\x00"+task)...)
}

func historyFenceKey(channel string, kind byte) []byte {
	// 专用表 0x1c01，不复用消息、索引或频道高水位键。
	return append([]byte{0x1c, 0x01, kind, 0, 1}, []byte(channel)...)
}

func (wk *wukongDB) historyFence(channel string, kind byte) (*HistoryFence, error) {
	return wk.readHistoryFence(channel, historyFenceKey(channel, kind))
}

func (wk *wukongDB) readHistoryFence(channel string, key []byte) (*HistoryFence, error) {
	data, closer, err := wk.defaultShardDB().Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	var fence HistoryFence
	if err = json.Unmarshal(data, &fence); err != nil {
		return nil, err
	}
	if fence.ChannelID != channel || !validHistoryFence(fence) {
		return nil, ErrHistoryConflict
	}
	return &fence, nil
}

func (wk *wukongDB) GetHistoryFence(channel string, kind uint8) (*HistoryFence, error) {
	if kind != 1 {
		return nil, nil
	}
	wk.historyFenceMu.RLock()
	defer wk.historyFenceMu.RUnlock()
	return wk.historyFence(channel, 1)
}

func sameHistoryIdentity(a, b HistoryFence) bool {
	return a.TaskID == b.TaskID && a.SourceUID == b.SourceUID && a.PeerUID == b.PeerUID && a.ChannelID == b.ChannelID
}

func (wk *wukongDB) putHistoryFence(fence HistoryFence) error {
	data, err := json.Marshal(fence)
	if err != nil {
		return err
	}
	return wk.defaultShardDB().Set(historyFenceKey(fence.ChannelID, 1), data, pebble.Sync)
}

func (wk *wukongDB) FreezeHistoryFence(fence HistoryFence) (HistoryFence, error) {
	return wk.freezeHistoryFence(fence, false)
}

func (wk *wukongDB) FreezeEmptyHistoryFence(fence HistoryFence) (HistoryFence, error) {
	return wk.freezeHistoryFence(fence, true)
}

func (wk *wukongDB) freezeHistoryFence(fence HistoryFence, requireEmpty bool) (HistoryFence, error) {
	if !validHistoryFence(fence) {
		return HistoryFence{}, ErrHistoryConflict
	}
	wk.historyFenceMu.Lock()
	defer wk.historyFenceMu.Unlock()
	old, err := wk.historyFence(fence.ChannelID, 1)
	if err != nil {
		return HistoryFence{}, err
	}
	if old != nil {
		if !sameHistoryIdentity(*old, fence) || (requireEmpty && old.HighWatermark != 0) {
			return HistoryFence{}, ErrHistoryConflict
		}
		return *old, nil
	}
	released, err := wk.readHistoryFence(fence.ChannelID, historyReleasedKey(fence.ChannelID, fence.TaskID))
	if err != nil {
		return HistoryFence{}, err
	}
	if released != nil && released.TaskID == fence.TaskID {
		return HistoryFence{}, ErrHistoryConflict
	}
	// 与底层 AppendMessages 共用锁，直接写入也不能越过高水位取样。
	fence.HighWatermark, _, err = wk.GetChannelLastMessageSeq(fence.ChannelID, 1)
	if err != nil {
		return HistoryFence{}, err
	}
	if fence.HighWatermark > math.MaxUint32 {
		return HistoryFence{}, ErrHistoryConflict
	}
	if requireEmpty {
		if _, err := wk.GetChannelClusterConfig(fence.ChannelID, 1); !errors.Is(err, ErrNotFound) {
			if err != nil {
				return HistoryFence{}, err
			}
			return HistoryFence{}, ErrHistoryUnsupported
		}
		if fence.HighWatermark != 0 {
			return HistoryFence{}, ErrHistoryConflict
		}
		// 同底层写锁内确认没有残留消息，不能把缺失频道配置等同于空历史。
		iter := wk.channelDb(fence.ChannelID, 1).NewIter(&pebble.IterOptions{
			LowerBound: key.NewMessagePrimaryKey(fence.ChannelID, 1, 0),
			UpperBound: key.NewMessagePrimaryKey(fence.ChannelID, 1, math.MaxUint64),
		})
		hasRecord := iter.First()
		readErr, closeErr := iter.Error(), iter.Close()
		if readErr != nil || closeErr != nil {
			return HistoryFence{}, errors.Join(readErr, closeErr)
		}
		if hasRecord {
			return HistoryFence{}, ErrHistoryConflict
		}
	}
	return fence, wk.putHistoryFence(fence)
}

func (wk *wukongDB) SetHistoryFence(fence HistoryFence) error {
	if !validHistoryFence(fence) {
		return ErrHistoryConflict
	}
	wk.historyFenceMu.Lock()
	defer wk.historyFenceMu.Unlock()
	// 槽 applied 检查点可能落后于业务库；旧任务日志重放不得复活已释放门禁。
	released, err := wk.readHistoryFence(fence.ChannelID, historyReleasedKey(fence.ChannelID, fence.TaskID))
	if err != nil {
		return err
	}
	if released != nil {
		if !sameHistoryIdentity(*released, fence) || released.HighWatermark != fence.HighWatermark {
			return ErrHistoryConflict
		}
		return nil
	}
	old, err := wk.historyFence(fence.ChannelID, 1)
	if err != nil {
		return err
	}
	if old != nil && (!sameHistoryIdentity(*old, fence) || old.HighWatermark != fence.HighWatermark) {
		return ErrHistoryConflict
	}
	return wk.putHistoryFence(fence)
}

func (wk *wukongDB) ReleaseHistoryFence(fence HistoryFence) error {
	if !validHistoryFence(fence) {
		return ErrHistoryConflict
	}
	wk.historyFenceMu.Lock()
	defer wk.historyFenceMu.Unlock()
	released, err := wk.readHistoryFence(fence.ChannelID, historyReleasedKey(fence.ChannelID, fence.TaskID))
	if err != nil {
		return err
	}
	if released != nil {
		if !sameHistoryIdentity(*released, fence) {
			return ErrHistoryConflict
		}
		return nil
	}
	old, err := wk.historyFence(fence.ChannelID, 1)
	if err != nil {
		return err
	}
	if old == nil {
		return ErrHistoryConflict
	}
	if !sameHistoryIdentity(*old, fence) {
		return ErrHistoryConflict
	}
	data, err := json.Marshal(old)
	if err != nil {
		return err
	}
	batch := wk.defaultShardDB().NewBatch()
	defer batch.Close()
	if err = batch.Set(historyReleasedKey(fence.ChannelID, fence.TaskID), data, nil); err != nil {
		return err
	}
	if err = batch.Delete(historyFenceKey(fence.ChannelID, 1), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}
