package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type groupMessagePurgeDB interface {
	CheckGroupMessagePurge(context.Context, string) error
	CheckGroupStreamHistory(string) error
	PurgeGroupMessages(context.Context, string) error
	GroupMessagesPurged(string) (bool, error)
}

func (s *Store) GroupMessagesPurged(channel string) (bool, error) {
	db, ok := s.wdb.(groupMessagePurgeDB)
	if !ok {
		return false, nil
	}
	if s.opts.HistorySourceChannel != nil {
		channel = s.opts.HistorySourceChannel(channel)
	}
	return db.GroupMessagesPurged(channel)
}

func (s *Store) lockGroupStreamApply(ctx context.Context, channel string) (func(), error) {
	// 不借用会话锁：槽应用在后台发起的普通事件提案期间也会调用这里。
	key := s.historyKey(channel, wkproto.ChannelTypeGroup)
	entry := s.groupStreamLocks.retain(key)
	if err := entry.config.Acquire(ctx, 1); err != nil {
		s.groupStreamLocks.release(key, entry)
		return nil, err
	}
	return func() {
		entry.config.Release(1)
		s.groupStreamLocks.release(key, entry)
	}, nil
}

// GroupPurge 只供本机管理入口使用；外部搜索门禁必须在预检及执行入口各检查一次。
func (s *Store) GroupPurge(ctx context.Context, id, commandID string, checkOnly bool) error {
	db, ok := s.wdb.(groupMessagePurgeDB)
	if !ok || s.opts.HistoryTopology == nil {
		return wkdb.ErrGroupMessagePurgeUnsupported
	}
	const kind = wkproto.ChannelTypeGroup
	unlock, err := s.historyWriteLockContext(ctx, id, kind)
	if err != nil {
		return err
	}
	defer unlock()
	if err = s.HistoryCapabilities(); err != nil {
		return err
	}
	if commandID == "" || commandID == id {
		return wkdb.ErrGroupMessagePurgeUnsupported
	}
	// 命令频道独立持久化。首版不能安全清完它时，必须在动主频道之前拒绝。
	checkCommandChannel := func() error {
		seq, _, err := s.wdb.GetChannelLastMessageSeq(commandID, kind)
		if err != nil {
			return err
		}
		if seq != 0 {
			return wkdb.ErrGroupMessagePurgeUnsupported
		}
		return db.CheckGroupStreamHistory(commandID)
	}
	if err := checkCommandChannel(); err != nil {
		return err
	}
	topology := s.opts.HistoryTopology(id, kind)
	if topology != nil && !errors.Is(topology, wkdb.ErrHistoryChannelMissing) {
		return topology
	}
	if errors.Is(topology, wkdb.ErrHistoryChannelMissing) {
		seq, _, err := s.wdb.GetChannelLastMessageSeq(id, kind)
		if err != nil {
			return err
		}
		if seq != 0 {
			return wkdb.ErrGroupMessagePurgeUnsupported
		}
	}
	if checkOnly {
		return db.CheckGroupMessagePurge(ctx, id)
	}
	if err := s.opts.HistoryDrain(ctx, id, kind); err != nil {
		return err
	}
	if err := s.opts.HistoryDrain(ctx, commandID, kind); err != nil {
		return err
	}
	if err := s.HistoryCapabilities(); err != nil {
		return err
	}
	unlockConfig, err := s.historyConfigLockContext(ctx, id, kind)
	if err != nil {
		return err
	}
	defer unlockConfig()
	if err := func() error {
		unlockStream, err := s.lockGroupStreamApply(ctx, id)
		if err != nil {
			return err
		}
		defer unlockStream()
		// 拒绝已落盘的命令频道流式历史；槽重放可能在排空消息后才到。
		if err := checkCommandChannel(); err != nil {
			return err
		}
		return db.PurgeGroupMessages(ctx, id)
	}(); err != nil {
		return err
	}
	if err := s.finishGroupPurge(ctx, id); err != nil {
		// 此后即使清理失败也不是预检拒绝，API 必须留住防重放标记。
		return fmt.Errorf("group purge needs continuation: %v", err)
	}
	if err := s.finishGroupPurge(ctx, commandID); err != nil {
		return fmt.Errorf("group command channel cleanup needs continuation: %v", err)
	}
	return nil
}

func (s *Store) finishGroupPurge(ctx context.Context, id string) error {
	const kind = wkproto.ChannelTypeGroup
	// 保留不含正文和成员的解散标记。失败可重试，已落盘的防重放标记不撤回。
	info, err := s.GetChannel(id, kind)
	if err != nil {
		return err
	}
	if !wkdb.IsEmptyChannelInfo(info) && !info.Disband {
		info.Disband = true
		if err := s.UpdateChannelInfo(info); err != nil {
			return err
		}
	}
	members, err := s.wdb.GetSubscribers(id, kind)
	if err != nil {
		return err
	}
	uids, err := s.GetChannelConversationLocalUsers(id, kind)
	if err != nil {
		return err
	}
	for _, member := range members {
		uids = append(uids, member.Uid)
	}
	seen := make(map[string]bool, len(uids))
	for _, uid := range uids {
		if uid == "" || seen[uid] {
			continue
		}
		seen[uid] = true
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.DeleteConversation(uid, id, kind); err != nil {
			return err
		}
	}
	if err := s.RemoveAllSubscriber(id, kind); err != nil {
		return err
	}
	if err := s.RemoveAllDenylist(id, kind); err != nil {
		return err
	}
	return s.RemoveAllAllowlist(id, kind)
}
