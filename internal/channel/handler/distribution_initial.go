package handler

import (
	"context"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"go.uber.org/zap"
)

const initialDistributionPath = "/wk/ingress/initialChannelDistribution"

func (h *Handler) distributeInitial(ctx *eventbus.ChannelContext) {
	if err := h.enqueueInitialDistribution(ctx.ChannelId, ctx.ChannelType, cloneDeliveryEvents(ctx.Events)); err != nil {
		h.Warn("首次分发未入队", zap.String("channelId", ctx.ChannelId), zap.Error(err))
	}
}

// 首次分发复用有界工作池；空接收者键与已有单用户补投队列分离。
// 与单用户补投相同，待办只存在内存中，不保证重启恢复或确认丢失时恰好一次。
func (h *Handler) enqueueInitialDistribution(channelId string, channelType uint8, events []*eventbus.Event) error {
	if len(events) == 0 {
		return nil
	}
	warned := false
	attempt := func(ctx context.Context) error {
		err := h.attemptInitialDistribution(ctx, channelId, channelType, events)
		if err != nil && ctx.Err() != context.Canceled && !warned {
			h.Warn("首次分发未完成，保留进程内待办", zap.String("channelId", channelId), zap.Error(err))
			warned = true
		}
		return err
	}
	h.deliveryMu.Lock()
	defer h.deliveryMu.Unlock()
	if h.deliveryStopped {
		return errDeliveryQueueStopped
	}
	if h.deliveryQueue == nil {
		h.deliveryQueue = newDeliveryQueue(context.Background(), 16, options.G.Channel.ProcessTimeout)
	}
	return h.deliveryQueue.enqueue(deliveryKey{channelId: channelId, channelType: channelType}, attempt)
}

func (h *Handler) attemptInitialDistribution(ctx context.Context, channelId string, channelType uint8, events []*eventbus.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	leader, err := service.Cluster.SlotLeaderIdOfChannel(channelId, channelType)
	if err != nil {
		return err
	}
	if leader == 0 {
		return errors.New("频道槽领导未就绪")
	}
	if !options.G.IsLocalNode(leader) {
		req := &forwardChannelEventReq{fromNode: options.G.Cluster.NodeId, channelId: channelId, channelType: channelType, events: events}
		body, err := req.encode()
		if err != nil {
			return err
		}
		// 新事件不能走旧单向通道：未升级节点会静默忽略未知类型。
		resp, err := service.Cluster.RequestWithContext(ctx, leader, initialDistributionPath, body)
		if err != nil {
			return err
		}
		if resp == nil || resp.Status != proto.StatusOK {
			return errors.New("新领导未确认接收首次分发")
		}
		return nil
	}
	// 标签锁和存储读取沿用既有同步实现，返回后必须重新核对取消及领导身份。
	tag, err := h.getOrMakeTagForLeader(channelId, channelType)
	if err != nil {
		return err
	}
	if tag == nil {
		return errors.New("首次分发未取得完整接收者标签")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := service.Cluster.SlotLeaderIdOfChannel(channelId, channelType)
	if err != nil {
		return err
	}
	if current != leader {
		return errors.New("创建分发标签期间频道槽领导已变化")
	}
	// 在完整路由前不先投递部分本地用户；确定责任后按这一快照完成一次全局分发。
	localEvents := cloneDeliveryEvents(events)
	for _, event := range localEvents {
		event.Type = eventbus.EventChannelDistribute
		event.TagKey = tag.Key
		event.Track.Record(track.PositionChannelDistribute)
	}
	h.distributeByTag(leader, tag, channelId, channelType, localEvents)
	return nil
}

func (h *Handler) acceptInitialDistribution(body []byte) error {
	req := &forwardChannelEventReq{}
	if err := req.decode(body); err != nil {
		return err
	}
	if req.channelId == "" || req.channelType == 0 || len(req.events) == 0 || options.G.IsOnlineCmdChannel(req.channelId) {
		return errors.New("首次分发请求必须包含普通频道和消息")
	}
	for _, event := range req.events {
		// EVENT 和不持久化消息允许序号、原因码为零，不按 SEND 的持久化结果误拒收。
		if event.Type != eventbus.EventChannelDistributeInitial || event.ToUid != "" || event.Conn == nil || event.Frame == nil {
			return errors.New("首次分发请求包含无效事件")
		}
	}
	leader, err := service.Cluster.SlotLeaderIdOfChannel(req.channelId, req.channelType)
	if err != nil {
		return err
	}
	if !options.G.IsLocalNode(leader) {
		// 配置暂不同步时拒绝接收，由来源退避重试，不能在 RPC 内递归转交。
		return fmt.Errorf("当前节点不是频道槽领导：%d", leader)
	}
	return h.enqueueInitialDistribution(req.channelId, req.channelType, req.events)
}
