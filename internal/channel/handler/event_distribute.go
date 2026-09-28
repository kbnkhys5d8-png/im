package handler

import (
	"errors"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/internal/types"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

// 跳过会话更新的频道类型
var skipConversationUpdateChannelTypes = []uint8{wkproto.ChannelTypeData, wkproto.ChannelTypeTemp, wkproto.ChannelTypeLive}

// 普通频道在 distributeInitial 完成全局路由后，此处只投递本节点接收者。
// 在线命令继续沿用既有全局/局部分发路径，不改变显式订阅者标签的处理。
func (h *Handler) distribute(ctx *eventbus.ChannelContext) {

	// 记录消息轨迹
	events := ctx.Events
	for _, event := range events {
		event.Track.Record(track.PositionChannelDistribute)
	}

	// 消息分发
	if options.G.IsOnlineCmdChannel(ctx.ChannelId) {
		// 分发在线cmd消息
		h.distributeOnlineCmd(ctx)
	} else {
		// 分发普通消息
		h.distributeCommon(ctx)
	}
}

// 普通消息分发
func (h *Handler) distributeCommon(ctx *eventbus.ChannelContext) {
	// 获取或创建tag
	tag, err := h.getCommonTag(ctx)
	if err != nil {
		h.Error("distributeCommon: get or make tag failed", zap.Error(err), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
		return
	}

	if tag == nil {
		h.Error("distributeCommon: get or make tag failed, tag is nil", zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
		return
	}

	// 打标签
	for _, event := range ctx.Events {
		event.TagKey = tag.Key
	}
	// 普通分发事件已经完成全局路由，即使本节点刚成为槽领导也只处理本地接收者。
	h.distributeLocalByTag(tag, ctx.ChannelId, ctx.ChannelType, ctx.Events)
}

// cmd消息分发
func (h *Handler) distributeOnlineCmd(ctx *eventbus.ChannelContext) {

	// // 按照tagKey分组事件
	tagKeyEvents := h.groupEventsByTagKey(ctx.Events)
	var err error
	for tagKey, events := range tagKeyEvents {
		if tagKey == "" {
			h.Warn("distributeOnlineCmd: tagKey is nil", zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
			continue
		}
		// 获取tag
		var tag *types.Tag
		if options.G.IsLocalNode(ctx.SlotLeaderId) {
			tag = service.TagManager.Get(tagKey)
		} else {
			tag, err = h.requestTag(ctx.SlotLeaderId, tagKey)
			if err != nil {
				h.Error("distributeOnlineCmd: request tag failed", zap.Error(err), zap.String("tagKey", tagKey), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
				continue
			}
		}
		if tag == nil {
			h.Error("distributeOnlineCmd: tag not found", zap.String("tagKey", tagKey), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
			continue
		}
		// 分发
		h.distributeByTag(ctx.SlotLeaderId, tag, ctx.ChannelId, ctx.ChannelType, events)
	}
}

// 按照tagKey分组事件
func (h *Handler) groupEventsByTagKey(events []*eventbus.Event) map[string][]*eventbus.Event {
	tagKeyEvents := make(map[string][]*eventbus.Event)
	for _, e := range events {
		tagKeyEvents[e.TagKey] = append(tagKeyEvents[e.TagKey], e)
	}
	return tagKeyEvents
}

func (h *Handler) distributeByTag(slotLeaderId uint64, tag *types.Tag, channelId string, channelType uint8, events []*eventbus.Event) {
	if slotLeaderId == 0 {
		h.Error("distributeByTag: leaderId is 0", zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		return
	}

	// 如果领导节点是本地节点，则负责转发给节点
	if options.G.IsLocalNode(slotLeaderId) {
		for _, node := range tag.Nodes {
			if node.LeaderId == options.G.Cluster.NodeId {
				continue
			}
			// 转发至对应节点
			h.distributeToNode(node.LeaderId, channelId, channelType, events)
		}
	}

	// 会话和整组转发只执行一次；每个接收者独立恢复并保留进程内待办。
	h.distributeLocalByTag(tag, channelId, channelType, events)
}

func (h *Handler) distributeLocalByTag(tag *types.Tag, channelId string, channelType uint8, events []*eventbus.Event) {
	localHasEvent := false
	for _, node := range tag.Nodes {
		if node.LeaderId == options.G.Cluster.NodeId && len(node.Uids) > 0 {
			localHasEvent = true
			break
		}
	}
	if !localHasEvent {
		return
	}
	if !h.isSkipConversationUpdate(channelType) {
		h.conversation(channelId, channelType, tag.Key, events)
	}
	pendingEvents := cloneDeliveryEvents(events)
	offline := newOfflineDeliveryBatch(pendingEvents)
	seen := make(map[string]struct{})
	for _, node := range tag.Nodes {
		if node.LeaderId != options.G.Cluster.NodeId {
			continue
		}
		for _, uid := range node.Uids {
			if _, exists := seen[uid]; exists {
				continue
			}
			seen[uid] = struct{}{}
			if err := h.enqueueDeliveryWithOffline(channelId, channelType, uid, pendingEvents, offline.add); err != nil {
				h.Warn("接收者分发未入队", zap.String("uid", uid), zap.Error(err))
			}
		}
	}
}

// 是否跳过会话更新
func (h *Handler) isSkipConversationUpdate(channelType uint8) bool {
	for _, t := range skipConversationUpdateChannelTypes {
		if t == channelType {
			return true
		}
	}
	return false
}

func (h *Handler) distributeToNode(leaderId uint64, channelId string, channelType uint8, events []*eventbus.Event) {
	for _, event := range events {
		if event.SourceNodeId != 0 && event.SourceNodeId == leaderId {
			h.Foucs("distributeToNode: sourceNode is forward node, not distribute", zap.Uint64("sourceNodeId", event.SourceNodeId), zap.Uint64("leaderId", leaderId), zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
			return
		}
	}
	h.forwardsToNode(leaderId, channelId, channelType, events)
}

func (h *Handler) getCommonTag(ctx *eventbus.ChannelContext) (*types.Tag, error) {

	// 如果当前节点是频道的领导者节点，则可以make tag
	if options.G.IsLocalNode(ctx.SlotLeaderId) {
		return h.getOrMakeTagForLeader(ctx.ChannelId, ctx.ChannelType)
	}
	tagKey := ctx.Events[0].TagKey

	// 判断当前的频道tag是否等于tagKey,如果不等于则删除旧的tag
	oldTagKey := service.TagManager.GetChannelTag(ctx.ChannelId, ctx.ChannelType)
	if oldTagKey != "" && oldTagKey != tagKey {
		service.TagManager.RetireTag(oldTagKey)
	}
	tag, err := h.commonService.GetOrRequestAndMakeTagWithLocal(ctx.ChannelId, ctx.ChannelType, tagKey)
	if err != nil {
		h.Error("processDiffuse: get tag failed", zap.Error(err), zap.String("fakeChannelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType), zap.String("tagKey", tagKey))
		return nil, err
	}
	if tag == nil {
		h.Error("processDiffuse: tag not found", zap.String("tagKey", tagKey), zap.String("channelId", ctx.ChannelId), zap.Uint8("channelType", ctx.ChannelType))
		return nil, nil
	}

	return tag, nil
}

// 请求tag
func (h *Handler) requestTag(leaderId uint64, tagKey string) (*types.Tag, error) {
	// 去领导节点请求
	tagResp, err := h.client.RequestTag(leaderId, &ingress.TagReq{
		TagKey: tagKey,
		NodeId: options.G.Cluster.NodeId,
	})
	if err != nil {
		h.Error("requestTag: get tag failed", zap.Error(err), zap.Uint64("leaderId", leaderId))
		return nil, err
	}
	tag, err := service.TagManager.MakeTagNotCacheWithTagKey(tagKey, tagResp.Uids)
	if err != nil {
		h.Error("requestTag: MakeTagNotCacheWithTagKey failed", zap.Error(err))
		return nil, err
	}
	return tag, nil
}

func (h *Handler) getOrMakeTagForLeader(fakeChannelId string, channelType uint8) (*types.Tag, error) {
	var (
		tag *types.Tag
		err error
	)
	err = service.TagManager.WithChannelTagLock(fakeChannelId, channelType, func() error {
		tagKey := service.TagManager.GetChannelTag(fakeChannelId, channelType)
		if tagKey != "" {
			tag = service.TagManager.Get(tagKey)
		}
		if tag == nil {
			// The channel lock orders rebuilds with membership invalidations.
			tag, err = h.makeChannelTag(fakeChannelId, channelType)
			if err != nil {
				h.Error("processMakeTag: makeTag failed", zap.Error(err), zap.String("tagKey", tagKey))
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tag, nil
}

func (h *Handler) makeChannelTag(fakeChannelId string, channelType uint8) (*types.Tag, error) {

	var (
		subscribers []string
	)

	if channelType == wkproto.ChannelTypePerson { // 个人频道
		var orgFakeChannelId = fakeChannelId
		if options.G.IsCmdChannel(fakeChannelId) {
			// 处理命令频道
			orgFakeChannelId = options.G.CmdChannelConvertOrginalChannel(fakeChannelId)
		}
		u1, u2 := options.GetFromUIDAndToUIDWith(orgFakeChannelId)
		subscribers = append(subscribers, u1, u2)
	} else if channelType == wkproto.ChannelTypeAgent { // agent频道
		u1, agent := options.GetUidAndAgentUIDWith(fakeChannelId)
		subscribers = append(subscribers, u1, agent)
	} else {
		// 如果是cmd频道需要去对应的源频道获取订阅者来制作tag
		if options.G.IsCmdChannel(fakeChannelId) {
			var err error
			subscribers, err = h.getCmdSubscribers(fakeChannelId, channelType)
			if err != nil {
				h.Error("processMakeTag: getCmdSubscribers failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
				return nil, err
			}
		} else {
			var err error
			subscribers, err = h.getSubscribers(fakeChannelId, channelType)
			if err != nil {
				h.Error("processMakeTag: getSubscribers failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
				return nil, err
			}
		}

	}
	tag, err := service.TagManager.MakeTag(subscribers)
	if err != nil {
		h.Error("processMakeTag: makeTag failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	service.TagManager.SetChannelTag(fakeChannelId, channelType, tag.Key)
	return tag, nil
}

func (h *Handler) getSubscribers(fakeChannelId string, channelType uint8) ([]string, error) {
	members, err := service.Store.GetSubscribers(fakeChannelId, channelType)
	if err != nil {
		h.Error("processMakeTag: getSubscribers failed", zap.Error(err), zap.String("fakeChannelId", fakeChannelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	var subscribers = make([]string, 0, len(members))
	for _, member := range members {
		subscribers = append(subscribers, member.Uid)
	}

	// 如果是客服频道，则从频道id中获取访客id
	if channelType == wkproto.ChannelTypeCustomerService {
		// 访客id
		visitorId, _ := options.G.GetCustomerServiceVisitorUID(fakeChannelId)
		if visitorId != "" {
			subscribers = append(subscribers, visitorId)
		}
	} else if channelType == wkproto.ChannelTypeVisitors { // 如果是访客频道，那么频道id就是访客的uid，所以这里需要加访客加入到订阅者里
		subscribers = append(subscribers, fakeChannelId)
	}
	return subscribers, nil
}

// 获取cmd频道的订阅者
func (h *Handler) getCmdSubscribers(channelId string, channelType uint8) ([]string, error) {
	// 原频道id
	orgFakeChannelId := options.G.CmdChannelConvertOrginalChannel(channelId)
	// 获取原频道的领导节点id
	leaderNode, err := service.Cluster.LeaderOfChannel(orgFakeChannelId, channelType)
	if err != nil {
		h.Error("processMakeTag: get leaderNode failed", zap.Error(err), zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		return nil, err
	}
	if leaderNode == nil {
		h.Error("processMakeTag: leaderNode is nil", zap.String("fakeChannelId", channelId), zap.Uint8("channelType", channelType))
		return nil, errors.New("leaderNode is nil")
	}
	leaderId := leaderNode.Id
	// 如果是本地节点，则直接获取订阅者
	var subscribers []string
	if options.G.IsLocalNode(leaderId) {
		members, err := service.Store.GetSubscribers(orgFakeChannelId, channelType)
		if err != nil {
			h.Error("processMakeTag: getSubscribers failed", zap.Error(err), zap.String("orgFakeChannelId", orgFakeChannelId), zap.Uint8("channelType", channelType))
			return nil, err
		}
		for _, member := range members {
			subscribers = append(subscribers, member.Uid)
		}
	} else {
		// 如果不是本地节点，则去请求领导节点获取订阅者
		subscribers, err = h.client.RequestSubscribers(leaderId, orgFakeChannelId, channelType)
		if err != nil {
			h.Error("processMakeTag: requestSubscribers failed", zap.Error(err), zap.String("orgFakeChannelId", orgFakeChannelId), zap.Uint8("channelType", channelType))
			return nil, err
		}
	}
	return subscribers, nil
}
