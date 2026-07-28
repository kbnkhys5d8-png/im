package wkdb

import (
	"errors"
	"fmt"

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
