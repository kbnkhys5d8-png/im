package wkdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"regexp"
	"sort"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
)

var (
	ErrGroupMessagePurgeUnsupported = errors.New("group message history cannot be purged safely")
	ErrGroupMessagesPurged          = errors.New("group message history is permanently purged")
	purgeGroupID                    = regexp.MustCompile(`^[0-9A-Za-z_-]{1,40}$`)
)

// 首版在单个写批次内删除正文和索引；超出边界时，不开始删除。
const maxGroupPurgeMessages = 5000

type groupPurgeMessage struct {
	seq       uint64
	id        uint64
	from      string
	client    string
	timestamp uint64
}

func purgedGroupKey(channel string) []byte {
	return append([]byte{0x1c, 0x02, 0, 2}, channel...)
}

func (wk *wukongDB) GroupMessagesPurged(channel string) (bool, error) {
	_, closer, err := wk.channelDb(channel, wkproto.ChannelTypeGroup).Get(purgedGroupKey(channel))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}

func (wk *wukongDB) filterGroupPurgeConversations(conversations []Conversation) ([]Conversation, func(), error) {
	var channels map[string]bool
	for _, c := range conversations {
		if c.ChannelType == wkproto.ChannelTypeGroup {
			if channels == nil {
				channels = make(map[string]bool)
			}
			channels[c.ChannelId] = false
		}
	}
	if len(channels) == 0 {
		return conversations, func() {}, nil
	}
	ids := make([]string, 0, len(channels))
	for id := range channels {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	unlocks := make([]func(), 0, len(ids))
	unlock := func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
	// 与写入清理标记使用同一把锁，防止缓冲的群会话晚于清理重新落盘。
	for _, id := range ids {
		unlocks = append(unlocks, wk.historyFenceLocks.acquire(id, false))
		purged, err := wk.GroupMessagesPurged(id)
		if err != nil {
			unlock()
			return nil, nil, err
		}
		channels[id] = purged
	}
	allowed := make([]Conversation, 0, len(conversations))
	for _, c := range conversations {
		if c.ChannelType != wkproto.ChannelTypeGroup || !channels[c.ChannelId] {
			allowed = append(allowed, c)
		}
	}
	return allowed, unlock, nil
}

// PurgeGroupMessages 只清理本节点的群消息存储。调用方须先验证拓扑和外部副本，
// 并停止该群的消息生产；该接口不负责群资料、通知队列或外部存储。
func (wk *wukongDB) PurgeGroupMessages(ctx context.Context, channel string) error {
	return wk.purgeGroupMessages(ctx, channel, false)
}

func (wk *wukongDB) CheckGroupMessagePurge(ctx context.Context, channel string) error {
	return wk.purgeGroupMessages(ctx, channel, true)
}

func (wk *wukongDB) CheckGroupStreamHistory(channel string) error {
	const kind = wkproto.ChannelTypeGroup
	iter := wk.channelDb(channel, kind).NewIter(nil)
	defer iter.Close()
	// 即使主消息已过期或缺失，也不能遗漏独立的流式历史。
	for _, prefix := range key.MessageEventChannelPrefixes(channel, kind) {
		if iter.SeekGE(prefix) && bytes.HasPrefix(iter.Key(), prefix) {
			return ErrGroupMessagePurgeUnsupported
		}
		if err := iter.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (wk *wukongDB) purgeGroupMessages(ctx context.Context, channel string, checkOnly bool) error {
	if !purgeGroupID.MatchString(channel) {
		return ErrGroupMessagePurgeUnsupported
	}
	unlock := wk.historyFenceLocks.acquire(channel, true)
	defer unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := wk.CheckGroupStreamHistory(channel); err != nil {
		return err
	}
	if purged, err := wk.GroupMessagesPurged(channel); err != nil || purged {
		return err
	}
	const kind = wkproto.ChannelTypeGroup
	searchLock := &wk.searchOutboxMu[wk.GetChannelShardIndex(channel, kind)]
	searchLock.Lock()
	defer searchLock.Unlock()

	db := wk.channelDb(channel, kind)
	lower := key.NewMessagePrimaryKey(channel, kind, 0)
	upper := key.NewMessagePrimaryKey(channel, kind, math.MaxUint64)
	outboxLower, outboxUpper, err := key.NewSearchOutboxChannelRange(channel, kind, 1)
	if err != nil {
		return err
	}
	floorKey, err := key.NewSearchOutboxFloorKey(channel, kind)
	if err != nil {
		return err
	}
	rows, err := wk.scanGroupPurgeMessages(ctx, db, channel, lower, upper)
	if err != nil {
		return err
	}

	batch := wk.channelBatchDb(channel, kind).NewBatch()
	for _, row := range rows {
		// 全局消息 ID 索引必须仍指向本群的原始记录，不能删掉其他会话的索引。
		primary := key.NewMessagePrimaryKey(channel, kind, row.seq)
		var identity [16]byte
		copy(identity[:], primary[4:])
		idKey := key.NewMessageIndexMessageIdKey(row.id)
		value, closer, err := db.Get(idKey)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				return ErrGroupMessagePurgeUnsupported
			}
			return err
		}
		matches := bytes.Equal(value, identity[:])
		if err := closer.Close(); err != nil {
			return err
		}
		if !matches {
			return ErrGroupMessagePurgeUnsupported
		}
		batch.Delete(idKey)
		batch.Delete(key.NewMessageSecondIndexFromUidKey(row.from, identity))
		batch.Delete(key.NewMessageSecondIndexClientMsgNoKey(row.client, identity))
		batch.Delete(key.NewMessageIndexTimestampKey(row.timestamp, identity))
	}
	if checkOnly {
		return ctx.Err()
	}
	batch.DeleteRange(lower, upper)
	batch.DeleteRange(outboxLower, outboxUpper)
	batch.Delete(floorKey)
	batch.Set(purgedGroupKey(channel), []byte{1})
	if err := wk.setChannelLastMessageSeq(channel, kind, 0, batch); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := batch.CommitWait(); err != nil {
		return err
	}
	wk.channelSeqCache.invalidateChannelLastSeq(channel, kind)
	return nil
}

func (wk *wukongDB) scanGroupPurgeMessages(ctx context.Context, db *pebble.DB, channel string, lower, upper []byte) ([]groupPurgeMessage, error) {
	iter := db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	defer iter.Close()
	var rows []groupPurgeMessage
	var row groupPurgeMessage
	var seen uint8
	for valid := iter.First(); valid; valid = iter.Next() {
		seq, column, err := key.ParseMessageColumnKey(iter.Key())
		if err != nil || seq == 0 || seq > math.MaxUint32 {
			return nil, ErrGroupMessagePurgeUnsupported
		}
		if seq != row.seq {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if row.seq != 0 {
				if seen != 63 || row.id == 0 || len(rows) >= maxGroupPurgeMessages {
					return nil, ErrGroupMessagePurgeUnsupported
				}
				rows = append(rows, row)
			}
			row, seen = groupPurgeMessage{seq: seq}, 0
		}
		value := iter.Value()
		switch column {
		case key.TableMessage.Column.ChannelId:
			if string(value) != channel {
				return nil, ErrGroupMessagePurgeUnsupported
			}
			seen |= 1
		case key.TableMessage.Column.ChannelType:
			if len(value) != 1 || value[0] != wkproto.ChannelTypeGroup {
				return nil, ErrGroupMessagePurgeUnsupported
			}
			seen |= 2
		case key.TableMessage.Column.MessageId:
			if len(value) != 8 {
				return nil, ErrGroupMessagePurgeUnsupported
			}
			row.id = binary.BigEndian.Uint64(value)
			seen |= 4
		case key.TableMessage.Column.FromUid:
			row.from = string(value)
			seen |= 8
		case key.TableMessage.Column.ClientMsgNo:
			row.client = string(value)
			seen |= 16
		case key.TableMessage.Column.Timestamp:
			if len(value) != 4 {
				return nil, ErrGroupMessagePurgeUnsupported
			}
			row.timestamp = uint64(int32(binary.BigEndian.Uint32(value)))
			seen |= 32
		case key.TableMessage.Column.StreamNo:
			if len(value) != 0 {
				return nil, ErrGroupMessagePurgeUnsupported
			}
		case key.TableMessage.Column.Setting:
			if len(value) != 1 || wkproto.Setting(value[0]).IsSet(wkproto.SettingStream) {
				return nil, ErrGroupMessagePurgeUnsupported
			}
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	if row.seq != 0 {
		if seen != 63 || row.id == 0 || len(rows) >= maxGroupPurgeMessages {
			return nil, ErrGroupMessagePurgeUnsupported
		}
		rows = append(rows, row)
	}
	return rows, nil
}
