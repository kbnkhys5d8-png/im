package wkdb

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

const MaxSearchOutboxPullLimit = 500

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
	Identity     SearchOutboxIdentity
	Message      Message
	AppliedIndex uint64
}

type SearchOutboxPullResult struct {
	Records         []SearchOutboxRecord
	Pending         uint64
	OldestCreatedAt int64
	AppliedBlocked  uint64
}

type searchOutboxChannel struct {
	id          string
	channelType uint8
}

type searchOutboxAck struct {
	identity SearchOutboxIdentity
	key      []byte
}

func (wk *wukongDB) AckSearchOutbox(identities []SearchOutboxIdentity) error {
	if len(identities) == 0 {
		return nil
	}

	unique := make([]SearchOutboxIdentity, 0, len(identities))
	seen := make(map[SearchOutboxIdentity]struct{}, len(identities))
	for index, identity := range identities {
		if err := identity.Validate(); err != nil {
			return fmt.Errorf("validate search outbox ack identity %d: %w", index, err)
		}
		if _, ok := seen[identity]; ok {
			continue
		}
		seen[identity] = struct{}{}
		unique = append(unique, identity)
	}

	acksByShard := make(map[uint32][]searchOutboxAck)
	shardIDs := make([]uint32, 0)
	for _, identity := range unique {
		keyBytes, err := key.NewSearchOutboxKey(
			identity.ChannelID,
			identity.ChannelType,
			identity.MessageSeq,
			identity.MessageID,
		)
		if err != nil {
			return fmt.Errorf("build search outbox ack key: %w", err)
		}
		shardID := wk.GetChannelShardIndex(identity.ChannelID, identity.ChannelType)
		if _, ok := acksByShard[shardID]; !ok {
			shardIDs = append(shardIDs, shardID)
		}
		acksByShard[shardID] = append(acksByShard[shardID], searchOutboxAck{
			identity: identity,
			key:      keyBytes,
		})
	}
	sort.Slice(shardIDs, func(left, right int) bool {
		return shardIDs[left] < shardIDs[right]
	})

	for _, shardID := range shardIDs {
		shardDB := wk.shardDBById(shardID)
		keysToDelete := make([][]byte, 0, len(acksByShard[shardID]))
		for _, ack := range acksByShard[shardID] {
			value, closer, err := shardDB.Get(ack.key)
			if errors.Is(err, pebble.ErrNotFound) {
				continue
			}
			if err != nil {
				return fmt.Errorf("load search outbox ack record: %w", err)
			}
			record, decodeErr := decodeSearchOutboxRecord(ack.key, value)
			closeErr := closer.Close()
			if decodeErr != nil {
				return fmt.Errorf("validate search outbox ack: %w", decodeErr)
			}
			if closeErr != nil {
				return fmt.Errorf("close search outbox ack record: %w", closeErr)
			}
			if record.Identity != ack.identity {
				return errors.New("search outbox ack identity differs from stored record")
			}
			keysToDelete = append(keysToDelete, ack.key)
		}
		if len(keysToDelete) == 0 {
			continue
		}

		batch := wk.shardBatchDBById(shardID).NewBatch()
		for _, keyBytes := range keysToDelete {
			batch.Delete(keyBytes)
		}
		if err := batch.CommitWait(); err != nil {
			return fmt.Errorf("commit search outbox ack for shard %d: %w", shardID, err)
		}
	}
	return nil
}

func (wk *wukongDB) PullSearchOutbox(limit int, maxBytes uint64) (SearchOutboxPullResult, error) {
	if limit < 1 || limit > MaxSearchOutboxPullLimit {
		return SearchOutboxPullResult{}, fmt.Errorf("search outbox pull limit must be between 1 and %d", MaxSearchOutboxPullLimit)
	}
	if maxBytes == 0 {
		return SearchOutboxPullResult{}, errors.New("search outbox byte budget must be positive")
	}

	result := SearchOutboxPullResult{
		Records: make([]SearchOutboxRecord, 0, limit),
	}
	appliedByChannel := make(map[searchOutboxChannel]uint64)
	var (
		usedBytes      uint64
		recordsStopped bool
	)
	for shardID := uint32(0); shardID < wk.shardNum; shardID++ {
		iter := wk.shardDBById(shardID).NewIter(&pebble.IterOptions{
			LowerBound: key.NewSearchOutboxLowKey(),
			UpperBound: key.NewSearchOutboxHighKey(),
		})
		for iter.First(); iter.Valid(); iter.Next() {
			keyBytes := iter.Key()
			value := append([]byte(nil), iter.Value()...)
			record, err := decodeSearchOutboxRecord(keyBytes, value)
			if err != nil {
				iter.Close()
				return SearchOutboxPullResult{}, err
			}
			if record.Message.Timestamp <= 0 {
				iter.Close()
				return SearchOutboxPullResult{}, errors.New("search outbox record has no server timestamp")
			}

			result.Pending++
			timestamp := int64(record.Message.Timestamp)
			if result.OldestCreatedAt == 0 || timestamp < result.OldestCreatedAt {
				result.OldestCreatedAt = timestamp
			}

			channel := searchOutboxChannel{
				id:          record.Identity.ChannelID,
				channelType: record.Identity.ChannelType,
			}
			applied, ok := appliedByChannel[channel]
			if !ok {
				applied, err = wk.GetChannelAppliedIndex(channel.id, channel.channelType)
				if err != nil {
					iter.Close()
					return SearchOutboxPullResult{}, fmt.Errorf("load durable applied index: %w", err)
				}
				appliedByChannel[channel] = applied
			}
			if applied == 0 || applied < record.Identity.MessageSeq {
				result.AppliedBlocked++
				continue
			}
			if recordsStopped {
				continue
			}
			if len(result.Records) == limit {
				recordsStopped = true
				continue
			}

			recordBytes := uint64(len(keyBytes)) + uint64(len(value))
			if recordBytes > maxBytes-usedBytes {
				if len(result.Records) == 0 {
					iter.Close()
					return SearchOutboxPullResult{}, ErrSearchOutboxByteBudget
				}
				recordsStopped = true
				continue
			}
			record.AppliedIndex = applied
			result.Records = append(result.Records, record)
			usedBytes += recordBytes
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return SearchOutboxPullResult{}, fmt.Errorf("iterate search outbox: %w", err)
		}
		if err := iter.Close(); err != nil {
			return SearchOutboxPullResult{}, fmt.Errorf("close search outbox iterator: %w", err)
		}
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

	visited := make(map[searchOutboxChannel]struct{})
	for shardID := uint32(0); shardID < wk.shardNum; shardID++ {
		iter := wk.shardDBById(shardID).NewIter(&pebble.IterOptions{
			LowerBound: key.NewSearchOutboxLowKey(),
			UpperBound: key.NewSearchOutboxHighKey(),
		})
		for iter.First(); iter.Valid(); iter.Next() {
			if err := ctx.Err(); err != nil {
				iter.Close()
				return err
			}
			channelID, channelType, _, _, err := key.ParseSearchOutboxKey(iter.Key())
			if err != nil {
				iter.Close()
				return fmt.Errorf("parse search outbox key: %w", err)
			}
			pending := searchOutboxChannel{id: channelID, channelType: channelType}
			if _, ok := visited[pending]; ok {
				continue
			}
			visited[pending] = struct{}{}
			if err := visit(Channel{ChannelId: channelID, ChannelType: channelType}); err != nil {
				iter.Close()
				return err
			}
		}
		if err := iter.Error(); err != nil {
			iter.Close()
			return fmt.Errorf("iterate search outbox channels: %w", err)
		}
		if err := iter.Close(); err != nil {
			return fmt.Errorf("close search outbox iterator: %w", err)
		}
	}
	return ctx.Err()
}

func (wk *wukongDB) GetSearchOutboxFloor(channelID string, channelType uint8) (floor uint64, enabled bool, err error) {
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
