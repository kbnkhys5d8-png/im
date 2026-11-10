package store

import (
	"context"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

// AppendMessages 追加消息
func (s *Store) AppendMessages(ctx context.Context, channelId string, channelType uint8, msgs []wkdb.Message) (types.ProposeRespSet, error) {
	unlock, err := s.historyReadLockContext(ctx, channelId, channelType)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if err := s.historyWritable(channelId, channelType); err != nil {
		return nil, err
	}

	if len(msgs) == 0 {
		return nil, nil
	}
	reqs := make([]types.ProposeReq, 0, len(msgs))
	for _, msg := range msgs {
		data, err := msg.Marshal()
		if err != nil {
			return nil, err
		}

		reqs = append(reqs, types.ProposeReq{
			Id:   uint64(msg.MessageID),
			Data: data,
		})
	}

	results, err := s.opts.Channel.ProposeBatchUntilAppliedTimeout(ctx, channelId, channelType, reqs)
	if err != nil {
		return nil, err
	}
	return results, nil
}

// AppendMessageChunks 追加消息chunks
func (s *Store) AppendMessageChunks(ctx context.Context, channelId string, channelType uint8, msgs []wkdb.Message) error {
	return s.WithHistorySend(channelId, channelType, func() error { return nil })
}

func (s *Store) LoadNextRangeMsgs(channelID string, channelType uint8, startMessageSeq, endMessageSeq uint64, limit int) ([]wkdb.Message, error) {
	unlock := s.historyReadLock(channelID, channelType)
	defer unlock()
	if err := s.historyWritable(channelID, channelType); err != nil {
		return nil, err
	}
	return s.wdb.LoadNextRangeMsgs(channelID, channelType, startMessageSeq, endMessageSeq, limit)
}

func (s *Store) LoadMsg(channelID string, channelType uint8, seq uint64) (wkdb.Message, error) {
	unlock := s.historyReadLock(channelID, channelType)
	defer unlock()
	if err := s.historyWritable(channelID, channelType); err != nil {
		return wkdb.EmptyMessage, err
	}
	return s.wdb.LoadMsg(channelID, channelType, seq)
}

func (s *Store) LoadLastMsgs(channelID string, channelType uint8, limit int) ([]wkdb.Message, error) {
	unlock := s.historyReadLock(channelID, channelType)
	defer unlock()
	if err := s.historyWritable(channelID, channelType); err != nil {
		return nil, err
	}
	return s.wdb.LoadLastMsgs(channelID, channelType, limit)
}

func (s *Store) LoadLastMsgsWithEnd(channelID string, channelType uint8, end uint64, limit int) ([]wkdb.Message, error) {
	unlock := s.historyReadLock(channelID, channelType)
	defer unlock()
	if err := s.historyWritable(channelID, channelType); err != nil {
		return nil, err
	}
	return s.wdb.LoadLastMsgsWithEnd(channelID, channelType, end, limit)
}

func (s *Store) LoadPrevRangeMsgs(channelID string, channelType uint8, start, end uint64, limit int) ([]wkdb.Message, error) {
	unlock := s.historyReadLock(channelID, channelType)
	defer unlock()
	if err := s.historyWritable(channelID, channelType); err != nil {
		return nil, err
	}
	return s.wdb.LoadPrevRangeMsgs(channelID, channelType, start, end, limit)
}

func (s *Store) GetLastMsgSeq(channelID string, channelType uint8) (uint64, error) {
	unlock := s.historyReadLock(channelID, channelType)
	defer unlock()
	if err := s.historyWritable(channelID, channelType); err != nil {
		return 0, err
	}
	seq, _, err := s.wdb.GetChannelLastMessageSeq(channelID, channelType)
	return seq, err
}

func (s *Store) GetMessagesOfNotifyQueue(count int) ([]wkdb.Message, error) {
	return s.wdb.GetMessagesOfNotifyQueue(count)
}

func (s *Store) AppendMessageOfNotifyQueue(messages []wkdb.Message) error {
	return s.wdb.AppendMessageOfNotifyQueue(messages)
}

func (s *Store) RemoveMessagesOfNotifyQueue(messageIDs []int64) error {
	return s.wdb.RemoveMessagesOfNotifyQueue(messageIDs)
}

func (s *Store) DeleteChannelAndClearMessages(channelID string, channelType uint8) error {
	return nil
}

// 搜索消息
func (s *Store) SearchMessages(req wkdb.MessageSearchReq) ([]wkdb.Message, error) {
	if req.ChannelId != "" && req.ChannelType != 0 {
		allowed, err := s.historyVisible(req.ChannelId, req.ChannelType)
		if err != nil {
			return nil, err
		}
		if !allowed {
			return nil, wkdb.ErrHistoryFenced
		}
	}
	limit := req.Limit
	var result []wkdb.Message
	for {
		rows, err := s.wdb.SearchMessages(req)
		if err != nil {
			return nil, err
		}
		result = append(result, rows...)
		visible := make([]wkdb.Message, 0, len(result))
		// 每次原始读取完成后复核全部候选，不能返回上一页已失效的来源记录。
		for _, msg := range result {
			allowed, err := s.historyVisible(msg.ChannelID, msg.ChannelType)
			if err != nil {
				return nil, err
			}
			if allowed {
				visible = append(visible, msg)
			}
		}
		result = visible
		if limit <= 0 || req.MessageId != 0 || req.ChannelId != "" || len(rows) < req.Limit || len(result) >= limit {
			return result, nil
		}
		// 全局页中过滤来源后继续扫描，不能把正常频道误显示成搜索已结束。
		cursor := rows[0].MessageID
		for _, msg := range rows {
			if (!req.Pre && msg.MessageID < cursor) || (req.Pre && msg.MessageID > cursor) {
				cursor = msg.MessageID
			}
		}
		if cursor <= 0 || (req.OffsetMessageId != 0 && ((!req.Pre && cursor >= req.OffsetMessageId) || (req.Pre && cursor <= req.OffsetMessageId))) {
			return result, nil
		}
		req.OffsetMessageId = cursor
		req.Limit = limit - len(result)
	}
}

// LoadMsgByClientMsgNo 通过 clientMsgNo 加载消息
func (s *Store) LoadMsgByClientMsgNo(channelId string, channelType uint8, clientMsgNo string) (wkdb.Message, error) {
	unlock := s.historyReadLock(channelId, channelType)
	defer unlock()
	if err := s.historyWritable(channelId, channelType); err != nil {
		return wkdb.EmptyMessage, err
	}
	return s.wdb.LoadMsgByClientMsgNo(channelId, channelType, clientMsgNo)
}

// GetUserLastMsgSeq 获取用户在指定频道内发送的最新一条消息的seq
func (s *Store) GetUserLastMsgSeq(fromUid string, channelId string, channelType uint8) (uint64, error) {
	unlock := s.historyReadLock(channelId, channelType)
	defer unlock()
	if err := s.historyWritable(channelId, channelType); err != nil {
		return 0, err
	}
	return s.wdb.GetUserLastMsgSeq(fromUid, channelId, channelType)
}

// LoadMsgsBatch 批量获取多个频道的消息
func (s *Store) LoadMsgsBatch(requests []wkdb.BatchMsgRequest) ([]wkdb.BatchMsgResponse, error) {
	rows, err := s.wdb.LoadMsgsBatch(requests)
	if err != nil {
		return nil, err
	}
	for i, row := range rows {
		allowed, err := s.historyVisible(row.ChannelId, row.ChannelType)
		if err != nil {
			return nil, err
		}
		if !allowed {
			rows[i].Messages = nil
		}
	}
	return rows, nil
}

// GetUserLastMsgSeqBatch 批量获取用户在多个频道的最后消息序号
func (s *Store) GetUserLastMsgSeqBatch(fromUid string, channels []wkdb.Channel) (map[string]uint64, error) {
	rows, err := s.wdb.GetUserLastMsgSeqBatch(fromUid, channels)
	if err != nil {
		return nil, err
	}
	for _, ch := range channels {
		allowed, err := s.historyVisible(ch.ChannelId, ch.ChannelType)
		if err != nil {
			return nil, err
		}
		if !allowed {
			delete(rows, ch.ChannelId+":"+string(ch.ChannelType))
		}
	}
	return rows, nil
}
