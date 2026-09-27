package wkdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

const MaxSearchOutboxPullLimit = 500

// 固定存储记录上限不随调用方分页预算变化，防止小预算把正常任务隔离。
const MaxSearchOutboxRecordBytes = uint64(5 * 1024 * 1024)

var (
	ErrSearchOutboxInvalidIdentity = errors.New("search outbox identity is invalid")
	ErrSearchOutboxByteBudget      = errors.New("search outbox record exceeds byte budget")
)

type SearchOutboxIdentity struct {
	ChannelID   string `json:"channel_id"`
	ChannelType uint8  `json:"channel_type"`
	MessageSeq  uint64 `json:"message_seq"`
	MessageID   int64  `json:"message_id"`
}

func (id SearchOutboxIdentity) Validate() error {
	if id.ChannelID == "" || len([]byte(id.ChannelID)) > key.MaxSearchOutboxChannelIDBytes || id.ChannelType == 0 || id.MessageSeq == 0 || id.MessageSeq > MaxMessageSequence || id.MessageID <= 0 {
		return ErrSearchOutboxInvalidIdentity
	}
	return nil
}

type SearchOutboxRecord struct {
	Epoch        uint64
	Identity     SearchOutboxIdentity
	Message      Message
	AppliedIndex uint64
}

type SearchOutboxPullResult struct {
	Records         []SearchOutboxRecord
	Pending         uint64
	Quarantined     uint64
	OldestCreatedAt int64
	AppliedBlocked  uint64
}

type searchOutboxChannel struct {
	id          string
	channelType uint8
}

// 旧入口不能绕过第二版协议的持久代次校验。
func (wk *wukongDB) AckSearchOutbox(identities []SearchOutboxIdentity) error {
	if len(identities) == 0 {
		return nil
	}
	return ErrSearchOutboxEpochRequired
}

func (wk *wukongDB) PullSearchOutbox(limit int, maxBytes uint64) (SearchOutboxPullResult, error) {
	if limit < 1 || limit > MaxSearchOutboxPullLimit {
		return SearchOutboxPullResult{}, fmt.Errorf("search outbox pull limit must be between 1 and %d", MaxSearchOutboxPullLimit)
	}
	if maxBytes == 0 {
		return SearchOutboxPullResult{}, errors.New("search outbox byte budget must be positive")
	}
	result := SearchOutboxPullResult{Records: make([]SearchOutboxRecord, 0, limit)}
	appliedByChannel := make(map[searchOutboxChannel]uint64)
	var usedBytes uint64
	recordsStopped := false
	unknownOldest := false
	for shardID := uint32(0); shardID < wk.shardNum; shardID++ {
		iter := wk.shardDBById(shardID).NewIter(&pebble.IterOptions{LowerBound: key.NewSearchOutboxLowKey(), UpperBound: key.NewSearchOutboxHighKey()})
		for iter.First(); iter.Valid(); iter.Next() {
			rawKey := append([]byte(nil), iter.Key()...)
			value := append([]byte(nil), iter.Value()...)
			recordBytes := uint64(len(rawKey)) + uint64(len(value))
			var record SearchOutboxRecord
			var err error
			reason := ""
			// 在解析超大内容前先按固定上限隔离，保留原始数据但不继续占用正常页。
			if recordBytes > MaxSearchOutboxRecordBytes {
				reason = "source_oversize"
			} else {
				record, err = decodeSearchOutboxRecord(rawKey, value)
				if err != nil {
					reason = "invalid_record"
				} else if record.Message.Timestamp <= 0 {
					reason = "invalid_timestamp"
				}
			}
			if reason != "" {
				removed, quarantineErr := wk.quarantineSearchOutboxCandidate(shardID, rawKey, value, reason)
				if quarantineErr != nil {
					iter.Close()
					return SearchOutboxPullResult{}, quarantineErr
				}
				if !removed {
					// 忙分片或过期快照未完成隔离，仍计入积压，不能冒充空队列。
					result.Pending++
					timestamp := int64(record.Message.Timestamp)
					if err != nil || timestamp <= 0 {
						unknownOldest = true
					} else if result.OldestCreatedAt == 0 || timestamp < result.OldestCreatedAt {
						result.OldestCreatedAt = timestamp
					}
				}
				continue
			}
			channel := searchOutboxChannel{id: record.Identity.ChannelID, channelType: record.Identity.ChannelType}
			applied, ok := appliedByChannel[channel]
			if !ok {
				applied, err = wk.GetChannelAppliedIndex(channel.id, channel.channelType)
				if err != nil {
					iter.Close()
					return SearchOutboxPullResult{}, fmt.Errorf("load durable applied index: %w", err)
				}
				appliedByChannel[channel] = applied
			}
			blocked := applied == 0 || applied < record.Identity.MessageSeq
			result.Pending++
			timestamp := int64(record.Message.Timestamp)
			if result.OldestCreatedAt == 0 || timestamp < result.OldestCreatedAt {
				result.OldestCreatedAt = timestamp
			}
			if blocked {
				result.AppliedBlocked++
				continue
			}
			// 单条仅超过调用者的小预算时继续找可容纳任务，不改变其持久状态。
			if recordBytes > maxBytes {
				continue
			}
			if recordsStopped {
				continue
			}
			if len(result.Records) == limit || recordBytes > maxBytes-usedBytes {
				recordsStopped = true
				continue
			}
			epoch, current, err := wk.pinSearchOutboxRecord(shardID, rawKey, value)
			if err != nil {
				iter.Close()
				return SearchOutboxPullResult{}, err
			}
			if !current {
				continue
			}
			record.AppliedIndex = applied
			record.Epoch = epoch
			result.Records = append(result.Records, record)
			usedBytes += recordBytes
		}
		iterErr := iter.Error()
		closeErr := iter.Close()
		if iterErr != nil {
			return SearchOutboxPullResult{}, fmt.Errorf("iterate search outbox: %w", iterErr)
		}
		if closeErr != nil {
			return SearchOutboxPullResult{}, closeErr
		}
	}
	quarantined, err := wk.countSearchOutboxQuarantine()
	if err != nil {
		return SearchOutboxPullResult{}, err
	}
	result.Quarantined = quarantined
	// 任意待处理记录的时间未知时，不能用其他记录的最早时间代表整个队列。
	if unknownOldest {
		result.OldestCreatedAt = 0
	}
	return result, nil
}

func (wk *wukongDB) ScanSearchOutboxChannels(ctx context.Context, visit func(Channel) error) error {
	if ctx == nil {
		return errors.New("search outbox scan context is nil")
	}
	if visit == nil {
		return errors.New("search outbox visit function is nil")
	}
	channels, err := wk.searchOutboxChannels(ctx)
	if err != nil {
		return err
	}
	// 回调可能唤醒 Raft 或访问网络，必须在释放存储锁后执行。
	for _, channel := range channels {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(channel); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (wk *wukongDB) searchOutboxChannels(ctx context.Context) ([]Channel, error) {
	visited := make(map[searchOutboxChannel]struct{})
	channels := make([]Channel, 0)
	for shardID := uint32(0); shardID < wk.shardNum; shardID++ {
		iter := wk.shardDBById(shardID).NewIter(&pebble.IterOptions{LowerBound: key.NewSearchOutboxLowKey(), UpperBound: key.NewSearchOutboxHighKey()})
		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				iter.Close()
				return nil, err
			}
			rawKey := append([]byte(nil), iter.Key()...)
			channelID, channelType, _, _, err := key.ParseSearchOutboxKey(rawKey)
			if err != nil {
				value := append([]byte(nil), iter.Value()...)
				if _, err := wk.quarantineSearchOutboxCandidate(shardID, rawKey, value, "invalid_record"); err != nil {
					iter.Close()
					return nil, err
				}
				continue
			}
			channel := searchOutboxChannel{id: channelID, channelType: channelType}
			if _, ok := visited[channel]; ok {
				continue
			}
			visited[channel] = struct{}{}
			channels = append(channels, Channel{ChannelId: channelID, ChannelType: channelType})
		}
		iterErr := iter.Error()
		closeErr := iter.Close()
		if iterErr != nil {
			return nil, fmt.Errorf("iterate search outbox channels: %w", iterErr)
		}
		if closeErr != nil {
			return nil, closeErr
		}
	}
	return channels, nil
}

func (wk *wukongDB) GetSearchOutboxFloor(channelID string, channelType uint8) (floor uint64, enabled bool, err error) {
	if !key.IsValidSearchOutboxChannelIdentity(channelID, channelType) {
		return 0, false, nil
	}
	keyBytes, err := key.NewSearchOutboxFloorKey(channelID, channelType)
	if err != nil {
		return 0, false, err
	}
	value, closer, err := wk.channelDb(channelID, channelType).Get(keyBytes)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	defer closer.Close()
	if len(value) != 8 {
		return 0, false, errors.New("search outbox floor is corrupt")
	}
	return wk.endian.Uint64(value), true, nil
}

func searchOutboxIdentityFromMessage(message Message) SearchOutboxIdentity {
	return SearchOutboxIdentity{
		ChannelID: message.ChannelID, ChannelType: message.ChannelType,
		MessageSeq: uint64(message.MessageSeq), MessageID: message.MessageID,
	}
}

func decodeSearchOutboxRecord(keyBytes, value []byte) (SearchOutboxRecord, error) {
	channelID, channelType, messageSeq, messageID, err := key.ParseSearchOutboxKey(keyBytes)
	if err != nil {
		return SearchOutboxRecord{}, fmt.Errorf("parse search outbox key: %w", err)
	}
	var message Message
	if err := message.Unmarshal(value); err != nil {
		return SearchOutboxRecord{}, fmt.Errorf("decode search outbox value: %w", err)
	}
	identity := SearchOutboxIdentity{ChannelID: channelID, ChannelType: channelType, MessageSeq: messageSeq, MessageID: messageID}
	if !message.SearchOutbox || searchOutboxIdentityFromMessage(message) != identity {
		return SearchOutboxRecord{}, errors.New("search outbox key and value identity differ")
	}
	return SearchOutboxRecord{Identity: identity, Message: message}, nil
}

func (wk *wukongDB) writeSearchOutbox(message Message, batch *Batch) error {
	identity := searchOutboxIdentityFromMessage(message)
	if err := identity.Validate(); err != nil {
		return err
	}
	keyBytes, err := key.NewSearchOutboxKey(identity.ChannelID, identity.ChannelType, identity.MessageSeq, identity.MessageID)
	if err != nil {
		return err
	}
	state, exists, err := wk.loadSearchOutboxState(wk.GetChannelShardIndex(identity.ChannelID, identity.ChannelType), keyBytes)
	if err != nil {
		return err
	}
	// 日志重放只重建原始聊天消息，不能复活已经隔离或确认完成的搜索任务。
	if state.Status != searchOutboxActive {
		return nil
	}
	if !exists {
		batch.Set(key.NewSearchOutboxStateKey(keyBytes), state.marshal())
	}
	value, err := message.Marshal()
	if err != nil {
		return fmt.Errorf("encode search outbox message: %w", err)
	}
	batch.Set(keyBytes, value)
	return nil
}

func (wk *wukongDB) ensureSearchOutboxFloor(channelID string, channelType uint8, firstSearchOutboxSeq uint64, batch *Batch) error {
	if firstSearchOutboxSeq == 0 {
		return errors.New("search outbox sequence is zero")
	}
	_, enabled, err := wk.GetSearchOutboxFloor(channelID, channelType)
	if err != nil || enabled {
		return err
	}
	keyBytes, err := key.NewSearchOutboxFloorKey(channelID, channelType)
	if err != nil {
		return err
	}
	value := make([]byte, 8)
	wk.endian.PutUint64(value, firstSearchOutboxSeq-1)
	batch.Set(keyBytes, value)
	return nil
}
