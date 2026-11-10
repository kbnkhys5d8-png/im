package channel

import (
	"context"
	"errors"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
)

// 不创建新频道配置，仅唤醒已有本机 leader 并等待冻结前的日志落盘应用。
func (s *Server) WaitForHistory(ctx context.Context, channel string, kind uint8) error {
	key := wkutil.ChannelToKey(channel, kind)
	rg := s.getRaftGroup(key)
	if rg.GetRaft(key) == nil {
		cfg, err := s.opts.DB.GetChannelClusterConfig(channel, kind)
		if errors.Is(err, wkdb.ErrNotFound) {
			// 不创建空频道；调用方须在底层写锁内再次确认无消息并写入零水位门禁。
			return nil
		}
		if err != nil {
			return err
		}
		if cfg.LeaderId != s.opts.NodeId {
			return wkdb.ErrHistoryUnsupported
		}
		if err := s.WakeLeaderIfNeed(cfg); err != nil {
			return err
		}
	}
	return rg.WaitForHistory(ctx, key)
}
