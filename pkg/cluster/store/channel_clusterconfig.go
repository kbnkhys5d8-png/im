package store

import "github.com/WuKongIM/WuKongIM/pkg/wkdb"

func (s *Store) SaveChannelClusterConfig(cfg wkdb.ChannelClusterConfig) (version uint64, err error) {
	// append 可能回调配置保存，不能重入频道读锁，也不能阻塞无关频道配置。
	unlock := s.historyConfigLock(cfg.ChannelId, cfg.ChannelType)
	defer unlock()
	// 已冻结来源不能迁往别的频道 leader；新成员必须先回放同槽门禁元数据。
	if err = s.historyWritable(cfg.ChannelId, cfg.ChannelType); err != nil {
		return 0, err
	}
	cfgData, err := cfg.Marshal()
	if err != nil {
		return 0, err
	}

	data, err := EncodeCMDChannelClusterConfigSave(cfg.ChannelId, cfg.ChannelType, cfgData)
	if err != nil {
		return 0, err
	}
	cmd := NewCMD(CMDChannelClusterConfigSave, data)
	cmdData, err := cmd.Marshal()
	if err != nil {
		return 0, err
	}
	slotId := s.opts.Slot.GetSlotId(cfg.ChannelId)
	resp, err := s.opts.Slot.ProposeUntilApplied(slotId, cmdData)
	if err != nil {
		return 0, err
	}
	return resp.Index, nil
}
