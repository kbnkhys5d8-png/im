package store

import (
	"context"
	"encoding/json"
	"errors"
	"math"

	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

const HistoryPageBytes = 4 * 1024 * 1024

type HistoryTopologyCluster interface {
	Nodes() []*nodetypes.Node
	Slots() []*nodetypes.Slot
	LeaderOfChannelForRead(string, uint8) (*nodetypes.Node, error)
	SlotLeaderIdOfChannel(string, uint8) (uint64, error)
}

func SingleNodeHistoryCapabilities(local uint64, cluster HistoryTopologyCluster) func() error {
	return func() error {
		nodes := cluster.Nodes()
		if local == 0 || len(nodes) != 1 || nodes[0] == nil || nodes[0].Id != local {
			return wkdb.ErrHistoryUnsupported
		}
		slots := cluster.Slots()
		if len(slots) == 0 {
			return wkdb.ErrHistoryUnsupported
		}
		for _, slot := range slots {
			if slot == nil || slot.Leader != local {
				return wkdb.ErrHistoryUnsupported
			}
		}
		return nil
	}
}

func (s *Store) HistoryCapabilities() error {
	if _, ok := s.wdb.(wkdb.HistoryFenceStore); !ok {
		return wkdb.ErrHistoryUnsupported
	}
	if s.opts.HistoryCapabilities == nil || s.opts.HistoryDrain == nil {
		return wkdb.ErrHistoryUnsupported
	}
	return s.opts.HistoryCapabilities()
}

// 查询实际成员，不以配置副本数或在线节点数代替严格单节点条件。
func SingleNodeHistoryTopology(local uint64, cluster HistoryTopologyCluster, missingConfig ...error) func(string, uint8) error {
	return func(channel string, kind uint8) error {
		if err := SingleNodeHistoryCapabilities(local, cluster)(); err != nil {
			return err
		}
		nodes := cluster.Nodes()
		if local == 0 || len(nodes) != 1 || nodes[0] == nil || nodes[0].Id != local {
			return wkdb.ErrHistoryUnsupported
		}
		slot, err := cluster.SlotLeaderIdOfChannel(channel, kind)
		if err != nil {
			return err
		}
		if slot != local {
			return wkdb.ErrHistoryUnsupported
		}
		leader, err := cluster.LeaderOfChannelForRead(channel, kind)
		if err != nil {
			for _, missing := range missingConfig {
				if errors.Is(err, missing) {
					return wkdb.ErrHistoryChannelMissing
				}
			}
			return err
		}
		if leader == nil || leader.Id != local {
			return wkdb.ErrHistoryUnsupported
		}
		return nil
	}
}

func (s *Store) historyFence(channel string, kind uint8) (*wkdb.HistoryFence, error) {
	if db, ok := s.wdb.(wkdb.HistoryFenceStore); ok {
		fence, err := db.GetHistoryFence(channel, kind)
		if err != nil || fence != nil {
			return fence, err
		}
		if kind == 1 && s.opts.HistorySourceChannel != nil {
			original := s.opts.HistorySourceChannel(channel)
			if original != channel {
				return db.GetHistoryFence(original, kind)
			}
		}
	}
	return nil, nil
}

func (s *Store) historyWritable(channel string, kind uint8) error {
	fence, err := s.historyFence(channel, kind)
	if err != nil {
		return err
	}
	if fence != nil {
		return wkdb.ErrHistoryFenced
	}
	return nil
}

// NoPersist 分发入队与普通持久化共用此锁，不能只在 HTTP 请求入口检查。
func (s *Store) WithHistorySend(channel string, kind uint8, fn func() error) error {
	unlock := s.historyReadLock(channel, kind)
	defer unlock()
	if err := s.historyWritable(channel, kind); err != nil {
		return err
	}
	return fn()
}

func (s *Store) historyTopology(channel string) error {
	if s.opts.HistoryTopology == nil {
		return wkdb.ErrHistoryUnsupported
	}
	return s.opts.HistoryTopology(channel, 1)
}

func (s *Store) historyCommand(ctx context.Context, kind CMDType, fence wkdb.HistoryFence) error {
	if s.opts.Slot == nil {
		return wkdb.ErrHistoryUnsupported
	}
	data, err := json.Marshal(fence)
	if err != nil {
		return err
	}
	data, err = NewCMD(kind, data).Marshal()
	if err != nil {
		return err
	}
	_, err = s.opts.Slot.ProposeUntilAppliedTimeout(ctx, s.opts.Slot.GetSlotId(fence.ChannelID), data)
	return err
}

func (s *Store) applyHistoryFence(cmd *CMD) error {
	db, ok := s.wdb.(wkdb.HistoryFenceStore)
	if !ok {
		return wkdb.ErrHistoryUnsupported
	}
	var fence wkdb.HistoryFence
	if err := json.Unmarshal(cmd.Data, &fence); err != nil {
		return err
	}
	if cmd.CmdType == CMDHistoryFenceSet {
		return db.SetHistoryFence(fence)
	}
	return db.ReleaseHistoryFence(fence)
}

func (s *Store) FreezeHistory(ctx context.Context, identity wkdb.HistoryFence) (wkdb.HistoryFence, error) {
	unlock, err := s.historyWriteLockContext(ctx, identity.ChannelID, 1)
	if err != nil {
		return wkdb.HistoryFence{}, err
	}
	defer unlock()
	if err := ctx.Err(); err != nil {
		return wkdb.HistoryFence{}, err
	}
	topologyErr := s.historyTopology(identity.ChannelID)
	empty := errors.Is(topologyErr, wkdb.ErrHistoryChannelMissing)
	if topologyErr != nil && !empty {
		return wkdb.HistoryFence{}, topologyErr
	}
	db, ok := s.wdb.(wkdb.HistoryFenceStore)
	if !ok {
		return wkdb.HistoryFence{}, wkdb.ErrHistoryUnsupported
	}
	if s.opts.HistoryDrain == nil {
		return wkdb.HistoryFence{}, wkdb.ErrHistoryUnsupported
	}
	if err := s.opts.HistoryDrain(ctx, identity.ChannelID, 1); err != nil {
		return wkdb.HistoryFence{}, err
	}
	unlockConfig, err := s.historyConfigLockContext(ctx, identity.ChannelID, 1)
	if err != nil {
		return wkdb.HistoryFence{}, err
	}
	defer unlockConfig()
	// 排空日志可能唤醒已有频道，须在配置锁内重新决定是否允许空来源分支。
	topologyErr = s.historyTopology(identity.ChannelID)
	empty = errors.Is(topologyErr, wkdb.ErrHistoryChannelMissing)
	if topologyErr != nil && !empty {
		return wkdb.HistoryFence{}, topologyErr
	}
	// 已接收 append 完成后才取样；槽日志失败也保留本地门禁，重试不得换水位。
	var fence wkdb.HistoryFence
	if empty {
		fence, err = db.FreezeEmptyHistoryFence(identity)
	} else {
		fence, err = db.FreezeHistoryFence(identity)
	}
	if err != nil {
		return wkdb.HistoryFence{}, err
	}
	if err = s.historyCommand(ctx, CMDHistoryFenceSet, fence); err != nil {
		return wkdb.HistoryFence{}, err
	}
	if err = s.historyTopology(identity.ChannelID); err != nil && !(errors.Is(err, wkdb.ErrHistoryChannelMissing) && fence.HighWatermark == 0) {
		return wkdb.HistoryFence{}, err
	}
	return fence, nil
}

func sameFenceIdentity(a, b wkdb.HistoryFence) bool {
	return a.TaskID == b.TaskID && a.SourceUID == b.SourceUID && a.PeerUID == b.PeerUID && a.ChannelID == b.ChannelID
}

func (s *Store) ReleaseHistory(ctx context.Context, identity wkdb.HistoryFence) error {
	unlock, err := s.historyWriteLockContext(ctx, identity.ChannelID, 1)
	if err != nil {
		return err
	}
	defer unlock()
	if _, ok := s.wdb.(wkdb.HistoryFenceStore); !ok {
		return wkdb.ErrHistoryUnsupported
	}
	old, err := s.historyFence(identity.ChannelID, 1)
	if err != nil {
		return err
	}
	if old != nil && !sameFenceIdentity(*old, identity) {
		return wkdb.ErrHistoryConflict
	}
	// 尚无门禁也必须耐久登记取消，拒绝尚未进入频道锁的迟到冻结请求。
	if err := s.historyTopology(identity.ChannelID); err != nil && !(errors.Is(err, wkdb.ErrHistoryChannelMissing) && (old == nil || old.HighWatermark == 0)) {
		return err
	}
	return s.historyCommand(ctx, CMDHistoryFenceRelease, identity)
}

type HistoryExportPage struct {
	Messages      []wkdb.Message
	HighWatermark uint64
	More          bool
}

func (s *Store) ExportHistory(ctx context.Context, identity wkdb.HistoryFence, after uint64, limit int) (HistoryExportPage, error) {
	unlock, err := s.historyReadLockContext(ctx, identity.ChannelID, 1)
	if err != nil {
		return HistoryExportPage{}, err
	}
	defer unlock()
	page := HistoryExportPage{Messages: []wkdb.Message{}}
	if limit < 1 || limit > 200 {
		return page, errors.New("invalid history page limit")
	}
	topologyErr := s.historyTopology(identity.ChannelID)
	if topologyErr != nil && !errors.Is(topologyErr, wkdb.ErrHistoryChannelMissing) {
		return page, topologyErr
	}
	fence, err := s.historyFence(identity.ChannelID, 1)
	if err != nil {
		return page, err
	}
	if fence == nil || !sameFenceIdentity(*fence, identity) {
		return page, wkdb.ErrHistoryConflict
	}
	page.HighWatermark = fence.HighWatermark
	if topologyErr != nil && fence.HighWatermark != 0 {
		return page, topologyErr
	}
	current, _, err := s.wdb.GetChannelLastMessageSeq(identity.ChannelID, 1)
	if err != nil {
		return page, err
	}
	if current != fence.HighWatermark {
		return page, wkdb.ErrHistoryConflict
	}
	if after > fence.HighWatermark || after > math.MaxUint32 {
		return page, errors.New("invalid history cursor")
	}
	if after == fence.HighWatermark {
		return page, nil
	}
	db, ok := s.wdb.(wkdb.HistoryFenceStore)
	if !ok {
		return page, wkdb.ErrHistoryUnsupported
	}
	next, size := after+1, 0
	for len(page.Messages) <= limit {
		if err := ctx.Err(); err != nil {
			return HistoryExportPage{}, err
		}
		// 逐条读取限制内存，不能先加载最多 201 条任意大小的正文。
		rows, err := db.LoadHistoryMessages(identity.ChannelID, next, fence.HighWatermark+1, 1)
		if err != nil {
			return HistoryExportPage{}, err
		}
		if len(rows) == 0 {
			break
		}
		msg := rows[0]
		seq := uint64(msg.MessageSeq)
		if seq < next || seq > fence.HighWatermark || msg.ChannelID != identity.ChannelID || msg.ChannelType != 1 {
			return HistoryExportPage{}, wkdb.ErrHistoryConflict
		}
		if msg.StreamNo != "" || msg.Setting.IsSet(wkproto.SettingStream) {
			return HistoryExportPage{}, wkdb.ErrHistoryStreamUnsupported
		}
		data, err := msg.Marshal()
		if err != nil {
			return HistoryExportPage{}, err
		}
		if len(page.Messages) == limit || size+len(data) > HistoryPageBytes {
			if len(page.Messages) == 0 {
				return HistoryExportPage{}, errors.New("history message exceeds page budget")
			}
			page.More = true
			break
		}
		page.Messages = append(page.Messages, msg)
		size += len(data)
		next = seq + 1
		if seq == fence.HighWatermark {
			break
		}
	}
	return page, nil
}
