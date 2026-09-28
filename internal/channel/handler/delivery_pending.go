package handler

import (
	"context"
	"errors"
	"fmt"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

const pendingDeliveryPath = "/wk/ingress/pendingChannelDelivery"

type deliverySession struct {
	nodeId    uint64
	connId    int64
	sessionId string
}

// SetRoutes 只注册集群内部补投接收入口，不增加客户端协议。
func (h *Handler) SetRoutes() {
	service.Cluster.Route(initialDistributionPath, func(c *wkserver.Context) {
		if err := h.acceptInitialDistribution(c.Body()); err != nil {
			c.WriteErr(err)
			return
		}
		// 仅确认首分发待办已进入本进程，不能作为客户端已收包的证明。
		c.WriteOk()
	})
	service.Cluster.Route(pendingDeliveryPath, func(c *wkserver.Context) {
		if err := h.acceptPendingDelivery(c.Body()); err != nil {
			c.WriteErr(err)
			return
		}
		// 确认的是本进程已接收待办，不代表手机收到，也不保证重启后保留。
		c.WriteOk()
	})
}

// Stop 先封闭入队，再取消并等待在途恢复和移交，防止关闭后重新创建队列。
func (h *Handler) Stop() {
	h.deliveryMu.Lock()
	h.deliveryStopped = true
	queue := h.deliveryQueue
	h.deliveryMu.Unlock()
	if queue != nil {
		queue.stop()
	}
}

// 同一批消息模板只复制一次，多个接收者共享只读模板，各自保存补投进度。
func cloneDeliveryEvents(events []*eventbus.Event) []*eventbus.Event {
	clones := make([]*eventbus.Event, len(events))
	for i, event := range events {
		clones[i] = event.Clone()
	}
	return clones
}

// enqueueDelivery 接收只读消息模板，同一频道、接收者的后续任务不能越过失败队首。
func (h *Handler) enqueueDelivery(channelId string, channelType uint8, uid string, events []*eventbus.Event) error {
	return h.enqueueDeliveryWithOffline(channelId, channelType, uid, events, newOfflineDeliveryBatch(events).add)
}

func (h *Handler) enqueueDeliveryWithOffline(channelId string, channelType uint8, uid string, events []*eventbus.Event, notifyOffline func(string)) error {
	if options.G.IsSystemUid(uid) || len(events) == 0 {
		return nil
	}
	sent := make(map[deliverySession]struct{})
	warned := false
	attempt := func(ctx context.Context) error {
		err := h.attemptDelivery(ctx, channelId, channelType, uid, events, sent, notifyOffline)
		if err != nil && ctx.Err() != context.Canceled && !warned {
			// 持续故障只记录首次失败，避免每轮补投重复刷出大量告警。
			h.Warn("用户消息分发未完成，保留进程内待办", zap.String("uid", uid), zap.String("channelId", channelId), zap.Error(err))
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
	return h.deliveryQueue.enqueue(deliveryKey{channelId: channelId, channelType: channelType, uid: uid}, attempt)
}

func (h *Handler) attemptDelivery(ctx context.Context, channelId string, channelType uint8, uid string, events []*eventbus.Event, sent map[deliverySession]struct{}, notifyOffline func(string)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	leader, err := service.Cluster.SlotLeaderIdOfChannel(uid, wkproto.ChannelTypePerson)
	if err != nil {
		return err
	}
	if leader == 0 {
		return errors.New("用户槽领导未就绪")
	}
	if !options.G.IsLocalNode(leader) {
		return h.handoffDelivery(ctx, leader, channelId, channelType, uid, events)
	}
	var recoveryErr error
	if service.ConnRecovery != nil {
		recoveryErr = service.ConnRecovery.Ensure(ctx, uid)
	}
	if err := ctx.Err(); err == context.Canceled {
		return err
	} else if err != nil {
		// 本轮超时仍向已知会话排入本地推送，但不能用过期的恢复结果判定离线。
		recoveryErr = err
	}
	// 恢复期间换主时保留待办，下一轮重新找领导，不能按旧目录判为离线。
	currentLeader, err := service.Cluster.SlotLeaderIdOfChannel(uid, wkproto.ChannelTypePerson)
	if err != nil {
		return err
	}
	if currentLeader != leader {
		return errors.New("用户槽领导已变化")
	}
	conns := eventbus.User.AuthedConnsByUid(uid)
	newConns := make([]*eventbus.Conn, 0, len(conns))
	masterOnline := false
	for _, conn := range conns {
		if conn.DeviceLevel == wkproto.DeviceLevelMaster {
			masterOnline = true
		}
		key := deliverySession{nodeId: conn.NodeId, connId: conn.ConnId, sessionId: conn.SessionId}
		if _, exists := sent[key]; !exists {
			newConns = append(newConns, conn)
		}
	}
	if len(newConns) > 0 {
		pushes := make([]*eventbus.Event, 0, len(events))
		for _, event := range events {
			push := event.Clone()
			push.ToUid, push.ToConns = uid, newConns
			push.ChannelId, push.ChannelType = channelId, channelType
			push.Type = eventbus.EventPushOnline
			pushes = append(pushes, push)
		}
		id := eventbus.Pusher.AddEvents(pushes)
		eventbus.Pusher.Advance(id)
		// 这里只去重同一个待办已经排入在线推送的会话，客户端收包仍走原有 ACK/重试。
		for _, conn := range newConns {
			sent[deliverySession{nodeId: conn.NodeId, connId: conn.ConnId, sessionId: conn.SessionId}] = struct{}{}
		}
	}
	if recoveryErr != nil {
		return recoveryErr
	}
	if !masterOnline && channelType != wkproto.ChannelTypeAgent {
		notifyOffline(uid)
	}
	return nil
}

func (h *Handler) handoffDelivery(ctx context.Context, leader uint64, channelId string, channelType uint8, uid string, events []*eventbus.Event) error {
	forwarded := cloneDeliveryEvents(events)
	for _, event := range forwarded {
		event.Type, event.ToUid = eventbus.EventChannelDistribute, uid
	}
	req := &forwardChannelEventReq{fromNode: options.G.Cluster.NodeId, channelId: channelId, channelType: channelType, events: forwarded}
	body, err := req.encode()
	if err != nil {
		return err
	}
	// 不能复用整组转发的 SourceNodeId 防环判断；新领导可能正好是消息最初的节点。
	resp, err := service.Cluster.RequestWithContext(ctx, leader, pendingDeliveryPath, body)
	if err != nil {
		return err
	}
	if resp == nil || resp.Status != proto.StatusOK {
		return errors.New("新领导未确认接收补投待办")
	}
	// 已取得明确的受理确认，不能因紧随其后的超时而重复移交。
	return nil
}

func (h *Handler) acceptPendingDelivery(body []byte) error {
	req := &forwardChannelEventReq{}
	if err := req.decode(body); err != nil {
		return err
	}
	if req.channelId == "" || req.channelType == 0 || len(req.events) == 0 {
		return errors.New("补投请求缺少频道或消息")
	}
	uid := req.events[0].ToUid
	if uid == "" || options.G.IsSystemUid(uid) {
		return errors.New("补投请求接收者无效")
	}
	for _, event := range req.events {
		if event.Type != eventbus.EventChannelDistribute || event.ToUid != uid || event.Conn == nil || event.Frame == nil {
			return errors.New("补投请求必须只包含同一接收者的完整分发事件")
		}
	}
	leader, err := service.Cluster.SlotLeaderIdOfChannel(uid, wkproto.ChannelTypePerson)
	if err != nil {
		return err
	}
	if !options.G.IsLocalNode(leader) {
		// 配置暂不同步时拒收并让来源退避，不在节点之间立即互相转发。
		return fmt.Errorf("当前节点不是接收者领导：%d", leader)
	}
	return h.enqueueDelivery(req.channelId, req.channelType, uid, req.events)
}
