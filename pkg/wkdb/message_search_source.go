package wkdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/cockroachdb/pebble"
)

const (
	// MaxMessageSequence is the largest sequence representable by RecvPacket.
	MaxMessageSequence             uint64 = 1<<32 - 1
	maxSearchSourceMessagePageSize        = 500
)

var ErrSearchSourceMessageResponseBudget = errors.New("search source message metadata exceeds response byte budget")

type SearchSourceMessage struct {
	Message        Message
	PayloadOmitted bool
}

// LoadNextRangeSearchSourceMessages deliberately does not reuse the ordinary
// message loaders. Both passes read one Pebble snapshot, and only values that
// fit the byte budget are copied during the decode pass.
func (wk *wukongDB) LoadNextRangeSearchSourceMessages(channelID string, channelType uint8, start, end uint64, limit int, limitSize uint64) (messages []SearchSourceMessage, retErr error) {
	if start == 0 || start > MaxMessageSequence {
		return nil, fmt.Errorf("search source start message sequence %d is outside uint32 range", start)
	}
	if end <= start || end > MaxMessageSequence+1 {
		return nil, fmt.Errorf("search source end message sequence %d is outside uint32 range", end)
	}
	if limit <= 0 || limit > maxSearchSourceMessagePageSize {
		return nil, fmt.Errorf("search source message limit must be between 1 and %d", maxSearchSourceMessagePageSize)
	}
	if limitSize == 0 {
		return nil, errors.New("search source byte limit must be positive")
	}

	snapshot := wk.channelDb(channelID, channelType).NewSnapshot()
	defer func() {
		if err := snapshot.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close search source snapshot: %w", err))
		}
		if retErr != nil {
			messages = nil
		}
	}()
	lower := key.NewMessagePrimaryKey(channelID, channelType, start)
	upper := key.NewMessagePrimaryKey(channelID, channelType, end)
	newIter := func() searchSourceMessageIterator {
		return snapshot.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	}
	return scanSearchSourceMessages(newIter, limit, limitSize)
}

type searchSourceMessageIterator interface {
	First() bool
	Next() bool
	Valid() bool
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}

type searchSourceMessagePlan struct {
	messageSeq     uint64
	payloadOmitted bool
}

type searchSourceMessageMeasurement struct {
	messageSeq       uint64
	metadataBytes    uint64
	payloadBytes     uint64
	columns          uint16
	hasPayload       bool
	metadataTooLarge bool
	payloadTooLarge  bool
}

func scanSearchSourceMessages(newIter func() searchSourceMessageIterator, limit int, limitSize uint64) ([]SearchSourceMessage, error) {
	plans, err := measureSearchSourceMessages(newIter(), limit, limitSize)
	if err != nil || len(plans) == 0 {
		return make([]SearchSourceMessage, 0), err
	}
	return decodeSearchSourceMessages(newIter(), plans)
}

func measureSearchSourceMessages(iter searchSourceMessageIterator, limit int, limitSize uint64) (plans []searchSourceMessagePlan, retErr error) {
	defer func() {
		retErr = closeSearchSourceMessageIterator(iter, retErr)
		if retErr != nil {
			plans = nil
		}
	}()
	plans = make([]searchSourceMessagePlan, 0, limit)
	var pageBytes uint64
	var row searchSourceMessageMeasurement
	var hasRow, stopped bool
	for iter.First(); iter.Valid(); iter.Next() {
		seq, column, err := parseSearchSourceMessageColumn(iter.Key())
		if err != nil {
			return nil, err
		}
		if !hasRow || row.messageSeq != seq {
			if hasRow {
				included, err := appendSearchSourceMessagePlan(&plans, &pageBytes, row, limitSize)
				if err != nil {
					return nil, err
				}
				if !included || len(plans) >= limit {
					stopped = true
					break
				}
			}
			row = searchSourceMessageMeasurement{messageSeq: seq}
			hasRow = true
		}
		value := iter.Value()
		if err := validateSearchSourceStoredColumn(seq, column, value); err != nil {
			return nil, err
		}
		columnBit, _ := searchSourceStoredColumnBit(column)
		if row.columns&columnBit != 0 {
			return nil, fmt.Errorf("search source message %d has duplicate column %x", seq, column)
		}
		row.columns |= columnBit
		if column == key.TableMessage.Column.Payload {
			row.hasPayload = true
			if !row.payloadTooLarge {
				row.payloadBytes, row.payloadTooLarge = addSearchSourceMeasuredBytes(row.payloadBytes, uint64(len(value)), limitSize)
			}
		} else if !row.metadataTooLarge {
			row.metadataBytes, row.metadataTooLarge = addSearchSourceMeasuredBytes(row.metadataBytes, uint64(len(value)), limitSize)
		}
	}
	if !stopped && hasRow {
		if _, err := appendSearchSourceMessagePlan(&plans, &pageBytes, row, limitSize); err != nil {
			return nil, err
		}
	}
	return plans, nil
}

func appendSearchSourceMessagePlan(plans *[]searchSourceMessagePlan, pageBytes *uint64, row searchSourceMessageMeasurement, limit uint64) (bool, error) {
	if row.columns != searchSourceRequiredColumnMask {
		return false, fmt.Errorf("search source message %d is missing required columns: present=%#x required=%#x", row.messageSeq, row.columns, searchSourceRequiredColumnMask)
	}
	if row.metadataTooLarge {
		return false, fmt.Errorf("%w: message_seq=%d", ErrSearchSourceMessageResponseBudget, row.messageSeq)
	}
	payloadOmitted := row.hasPayload && (row.payloadTooLarge || !searchSourceBytesFit(limit, row.metadataBytes, row.payloadBytes))
	messageBytes := row.metadataBytes
	if !payloadOmitted {
		messageBytes += row.payloadBytes
	}
	if !searchSourceBytesFit(limit, *pageBytes, messageBytes) {
		return false, nil
	}
	*plans = append(*plans, searchSourceMessagePlan{messageSeq: row.messageSeq, payloadOmitted: payloadOmitted})
	*pageBytes += messageBytes
	return true, nil
}

func decodeSearchSourceMessages(iter searchSourceMessageIterator, plans []searchSourceMessagePlan) (messages []SearchSourceMessage, retErr error) {
	defer func() {
		retErr = closeSearchSourceMessageIterator(iter, retErr)
		if retErr != nil {
			messages = nil
		}
	}()
	messages = make([]SearchSourceMessage, len(plans))
	for i, plan := range plans {
		messages[i].Message.MessageSeq = uint32(plan.messageSeq)
		messages[i].PayloadOmitted = plan.payloadOmitted
	}
	planIndex := 0
	planSeen := false
	for iter.First(); iter.Valid(); iter.Next() {
		seq, column, err := parseSearchSourceMessageColumn(iter.Key())
		if err != nil {
			return nil, err
		}
		for planIndex < len(plans) && seq > plans[planIndex].messageSeq {
			if !planSeen {
				return nil, fmt.Errorf("search source message %d disappeared while decoding snapshot", plans[planIndex].messageSeq)
			}
			planIndex++
			planSeen = false
		}
		if planIndex >= len(plans) {
			break
		}
		if seq < plans[planIndex].messageSeq {
			continue
		}
		planSeen = true
		if err := decodeSearchSourceStoredColumn(&messages[planIndex], column, iter.Value()); err != nil {
			return nil, fmt.Errorf("decode search source message %d: %w", seq, err)
		}
	}
	if planIndex < len(plans) && planSeen {
		planIndex++
	}
	if planIndex != len(plans) {
		return nil, errors.New("search source snapshot ended before planned messages were decoded")
	}
	return messages, nil
}

func parseSearchSourceMessageColumn(rawKey []byte) (uint64, [2]byte, error) {
	seq, column, err := key.ParseMessageColumnKey(rawKey)
	if err != nil {
		return 0, [2]byte{}, fmt.Errorf("parse search source message column: %w", err)
	}
	if seq == 0 || seq > MaxMessageSequence {
		return 0, [2]byte{}, fmt.Errorf("search source message sequence %d is outside uint32 range", seq)
	}
	return seq, column, nil
}

func validateSearchSourceStoredColumn(seq uint64, column [2]byte, value []byte) error {
	if _, ok := searchSourceStoredColumnBit(column); !ok {
		return fmt.Errorf("search source message %d has unknown column %x", seq, column)
	}
	want := -1
	switch column {
	case key.TableMessage.Column.Header, key.TableMessage.Column.Setting, key.TableMessage.Column.ChannelType:
		want = 1
	case key.TableMessage.Column.Expire, key.TableMessage.Column.Timestamp:
		want = 4
	case key.TableMessage.Column.MessageId, key.TableMessage.Column.MessageSeq, key.TableMessage.Column.Term:
		want = 8
	}
	if want >= 0 && len(value) != want {
		return fmt.Errorf("search source message %d has invalid column %x length %d", seq, column, len(value))
	}
	if column == key.TableMessage.Column.MessageSeq && binary.BigEndian.Uint64(value) != seq {
		return fmt.Errorf("search source message key sequence %d differs from stored sequence %d", seq, binary.BigEndian.Uint64(value))
	}
	return nil
}

const (
	searchSourceColumnHeader uint16 = 1 << iota
	searchSourceColumnSetting
	searchSourceColumnExpire
	searchSourceColumnMessageID
	searchSourceColumnMessageSeq
	searchSourceColumnClientMsgNo
	searchSourceColumnTimestamp
	searchSourceColumnChannelID
	searchSourceColumnChannelType
	searchSourceColumnTopic
	searchSourceColumnFromUID
	searchSourceColumnPayload
	searchSourceColumnTerm
	searchSourceColumnStreamNo
	searchSourceRequiredColumnMask = (1 << iota) - 1
)

func searchSourceStoredColumnBit(column [2]byte) (uint16, bool) {
	switch column {
	case key.TableMessage.Column.Header:
		return searchSourceColumnHeader, true
	case key.TableMessage.Column.Setting:
		return searchSourceColumnSetting, true
	case key.TableMessage.Column.Expire:
		return searchSourceColumnExpire, true
	case key.TableMessage.Column.MessageId:
		return searchSourceColumnMessageID, true
	case key.TableMessage.Column.MessageSeq:
		return searchSourceColumnMessageSeq, true
	case key.TableMessage.Column.ClientMsgNo:
		return searchSourceColumnClientMsgNo, true
	case key.TableMessage.Column.Timestamp:
		return searchSourceColumnTimestamp, true
	case key.TableMessage.Column.ChannelId:
		return searchSourceColumnChannelID, true
	case key.TableMessage.Column.ChannelType:
		return searchSourceColumnChannelType, true
	case key.TableMessage.Column.Topic:
		return searchSourceColumnTopic, true
	case key.TableMessage.Column.FromUid:
		return searchSourceColumnFromUID, true
	case key.TableMessage.Column.Payload:
		return searchSourceColumnPayload, true
	case key.TableMessage.Column.Term:
		return searchSourceColumnTerm, true
	case key.TableMessage.Column.StreamNo:
		return searchSourceColumnStreamNo, true
	default:
		return 0, false
	}
}

func decodeSearchSourceStoredColumn(row *SearchSourceMessage, column [2]byte, value []byte) error {
	seq := uint64(row.Message.MessageSeq)
	if err := validateSearchSourceStoredColumn(seq, column, value); err != nil {
		return err
	}
	switch column {
	case key.TableMessage.Column.Header:
		row.Message.Framer = wkproto.FramerFromUint8(value[0])
	case key.TableMessage.Column.Setting:
		row.Message.Setting = wkproto.Setting(value[0])
	case key.TableMessage.Column.Expire:
		row.Message.Expire = binary.BigEndian.Uint32(value)
	case key.TableMessage.Column.MessageId:
		row.Message.MessageID = int64(binary.BigEndian.Uint64(value))
	case key.TableMessage.Column.ClientMsgNo:
		row.Message.ClientMsgNo = string(value)
	case key.TableMessage.Column.StreamNo:
		row.Message.StreamNo = string(value)
	case key.TableMessage.Column.Timestamp:
		row.Message.Timestamp = int32(binary.BigEndian.Uint32(value))
	case key.TableMessage.Column.ChannelId:
		row.Message.ChannelID = string(value)
	case key.TableMessage.Column.ChannelType:
		row.Message.ChannelType = value[0]
	case key.TableMessage.Column.Topic:
		row.Message.Topic = string(value)
	case key.TableMessage.Column.FromUid:
		row.Message.FromUID = string(value)
	case key.TableMessage.Column.Payload:
		if !row.PayloadOmitted {
			row.Message.Payload = append([]byte(nil), value...)
			if len(value) == 0 {
				row.Message.Payload = make([]byte, 0)
			}
		}
	case key.TableMessage.Column.Term:
		row.Message.Term = binary.BigEndian.Uint64(value)
	}
	return nil
}

func addSearchSourceMeasuredBytes(current, addition, limit uint64) (uint64, bool) {
	if !searchSourceBytesFit(limit, current, addition) {
		return current, true
	}
	return current + addition, false
}

func searchSourceBytesFit(limit uint64, parts ...uint64) bool {
	remaining := limit
	for _, part := range parts {
		if part > remaining {
			return false
		}
		remaining -= part
	}
	return true
}

func closeSearchSourceMessageIterator(iter searchSourceMessageIterator, current error) error {
	if err := iter.Error(); err != nil {
		current = errors.Join(current, fmt.Errorf("iterate search source messages: %w", err))
	}
	if err := iter.Close(); err != nil {
		current = errors.Join(current, fmt.Errorf("close search source message iterator: %w", err))
	}
	return current
}
