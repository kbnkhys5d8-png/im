package handler

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type initialChannelCapture struct{ events []*eventbus.Event }

func (c *initialChannelCapture) AddEvent(_ string, _ uint8, e *eventbus.Event) {
	c.events = append(c.events, e)
}
func (*initialChannelCapture) Advance(string, uint8) {}

type initialTagManager struct {
	service.ITagManager
	tag    *types.Tag
	before func() error
}

func (m *initialTagManager) WithChannelTagLock(_ string, _ uint8, fn func() error) error {
	if m.before != nil {
		if err := m.before(); err != nil {
			return err
		}
	}
	return fn()
}
func (m *initialTagManager) GetChannelTag(string, uint8) string { return m.tag.Key }
func (m *initialTagManager) Get(string) *types.Tag              { return m.tag }

func TestPersistSeparatesInitialAndLocalDistribution(t *testing.T) {
	h, _, _ := setupDistributeDelivery(t)
	oldChannel := eventbus.Channel
	capture := &initialChannelCapture{}
	eventbus.RegisterChannel(capture)
	t.Cleanup(func() { eventbus.Channel = oldChannel })
	for _, channel := range []string{"chat", options.G.Channel.OnlineCmdChannelId} {
		capture.events = nil
		e := testPersistEvent("persisted", wkproto.ReasonSuccess, true)
		h.persist(&eventbus.ChannelContext{ChannelId: channel, ChannelType: wkproto.ChannelTypeData, Events: []*eventbus.Event{e}})
		if len(capture.events) != 1 {
			t.Fatalf("分发事件数量=%d", len(capture.events))
		}
		got := capture.events[0].Type
		if channel == options.G.Channel.OnlineCmdChannelId {
			if got != eventbus.EventChannelDistribute {
				t.Fatal("在线命令必须保留既有路径")
			}
		} else if got == eventbus.EventChannelDistribute {
			t.Fatal("首次全局分发不能与节点局部分发共用类型")
		}
	}
}

func TestInitialDistributionAfterSlotChangeUsesAcknowledgedHandoff(t *testing.T) {
	h, cluster := setupDeliveryHandoff(t)
	h.routes()
	var calls atomic.Int32
	cluster.request = func(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
		if node != 2 || path != "/wk/ingress/initialChannelDistribution" {
			return nil, errors.New("首次分发交接路径错误")
		}
		req := &forwardChannelEventReq{}
		if err := req.decode(body); err != nil {
			return nil, err
		}
		if req.events[0].MessageSeq != 7 || req.events[0].Type == eventbus.EventChannelOnSend {
			return nil, errors.New("不能重新落库")
		}
		calls.Add(1)
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	// 先由真实持久化后置步骤创建事件，再模拟下一轮读取到新的槽领导。
	oldChannel := eventbus.Channel
	capture := &initialChannelCapture{}
	eventbus.RegisterChannel(capture)
	t.Cleanup(func() { eventbus.Channel = oldChannel })
	e := testPersistEvent("first", wkproto.ReasonSuccess, true)
	e.MessageSeq = 7
	h.persist(&eventbus.ChannelContext{ChannelId: "chat", ChannelType: wkproto.ChannelTypeData, Events: []*eventbus.Event{e}})
	if capture.events[0].Type == eventbus.EventChannelDistribute {
		t.Fatal("换主后仍是无法交接的局部分发类型")
	}
	h.OnEvent(&eventbus.ChannelContext{ChannelId: "chat", ChannelType: wkproto.ChannelTypeData, SlotLeaderId: 2, EventType: capture.events[0].Type, Events: capture.events})
	if h.deliveryQueue == nil {
		t.Fatal("换主后的首次分发未保留待办")
	}
	waitDeliveryHandoff(t, func() bool { return h.deliveryQueue.pendingCount() == 0 })
	if calls.Load() != 1 {
		t.Fatalf("确认交接次数=%d", calls.Load())
	}
}

func TestLocalDistributionDoesNotFanOutAfterBecomingChannelLeader(t *testing.T) {
	h, users, pusher := setupDistributeDelivery(t)
	oldTags := service.TagManager
	service.TagManager = &initialTagManager{tag: &types.Tag{Key: "tag", Nodes: []*types.Node{{LeaderId: 1, Uids: []string{"local"}}, {LeaderId: 2, Uids: []string{"remote"}}}}}
	t.Cleanup(func() { h.Stop(); service.TagManager = oldTags })
	cluster := &distributeForwardCluster{}
	service.Cluster = cluster
	service.ConnRecovery = nil
	users.set("local", distributeConn("local", 1, 1, "local"))
	e := testPersistEvent("local-only", wkproto.ReasonSuccess, false)
	e.Type = eventbus.EventChannelDistribute
	h.distribute(&eventbus.ChannelContext{ChannelId: "chat", ChannelType: wkproto.ChannelTypeData, SlotLeaderId: 1, Events: []*eventbus.Event{e}})
	waitDistributeDeliveryEmpty(t, h)
	if cluster.calls.Load() != 0 || len(pusher.snapshot()) != 1 {
		t.Fatalf("局部分发不能再次扩散：转发=%d 推送=%d", cluster.calls.Load(), len(pusher.snapshot()))
	}
}

func TestOnlineCommandDistributionKeepsExistingFanout(t *testing.T) {
	h, cluster, _, _ := setupInitialFanout(t, false)
	e := testPersistEvent("online-cmd", wkproto.ReasonSuccess, true)
	e.Type, e.TagKey = eventbus.EventChannelDistribute, "initial"
	h.distribute(&eventbus.ChannelContext{
		ChannelId: options.G.Channel.OnlineCmdChannelId, ChannelType: wkproto.ChannelTypeData,
		SlotLeaderId: 1, Events: []*eventbus.Event{e},
	})
	if cluster.sends.Load() != 1 || cluster.badType.Load() {
		t.Fatal("在线命令的既有全局分发不应改成局部分发")
	}
}

func TestInitialDistributionRetainsFailedHandoffAndUsesLatestLeader(t *testing.T) {
	h, cluster := setupDeliveryHandoff(t)
	var calls atomic.Int32
	cluster.request = func(ctx context.Context, node uint64, _ string, body []byte) (*proto.Response, error) {
		if _, ok := ctx.Deadline(); !ok {
			return nil, errors.New("请求缺少独立超时")
		}
		req := &forwardChannelEventReq{}
		if err := req.decode(body); err != nil {
			return nil, err
		}
		if req.events[0].Type != eventbus.EventChannelDistributeInitial || req.events[0].ToUid != "" {
			return nil, errors.New("交接阶段错误")
		}
		switch calls.Add(1) {
		case 1:
			if node != 2 {
				t.Errorf("第一次目标=%d", node)
			}
			cluster.leader.Store(3)
			return nil, errors.New("路由不存在或网络失败")
		case 2:
			if node != 3 {
				t.Errorf("重试应重新查询领导，目标=%d", node)
			}
			return &proto.Response{Status: proto.StatusError}, nil
		default:
			return &proto.Response{Status: proto.StatusOK}, nil
		}
	}
	e := testPersistEvent("handoff", wkproto.ReasonSuccess, false)
	e.Type, e.MessageSeq, e.SourceNodeId = eventbus.EventChannelDistributeInitial, 8, 3
	if err := h.enqueueInitialDistribution("chat", wkproto.ChannelTypeData, []*eventbus.Event{e}); err != nil {
		t.Fatal(err)
	}
	waitDeliveryHandoff(t, func() bool { return h.deliveryQueue.pendingCount() == 0 })
	if calls.Load() != 3 || e.Type != eventbus.EventChannelDistributeInitial || e.MessageSeq != 8 {
		t.Fatalf("交接丢失或改写模板：调用=%d 事件=%+v", calls.Load(), e)
	}
}

func TestInitialDistributionStopCancelsHandoff(t *testing.T) {
	h, cluster := setupDeliveryHandoff(t)
	entered, exited := make(chan struct{}), make(chan struct{})
	cluster.request = func(ctx context.Context, _ uint64, _ string, _ []byte) (*proto.Response, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	}
	e := testPersistEvent("cancel", wkproto.ReasonSuccess, false)
	e.Type = eventbus.EventChannelDistributeInitial
	if err := h.enqueueInitialDistribution("chat", wkproto.ChannelTypeData, []*eventbus.Event{e}); err != nil {
		t.Fatal(err)
	}
	waitDistributeRecoverySignal(t, entered, "首分发请求开始")
	h.Stop()
	select {
	case <-exited:
	default:
		t.Fatal("停止未等待交接请求退出")
	}
	if err := h.enqueueInitialDistribution("chat", wkproto.ChannelTypeData, []*eventbus.Event{e}); !errors.Is(err, errDeliveryQueueStopped) {
		t.Fatalf("停止后重新入队：%v", err)
	}
}

func TestInitialDistributionAcknowledgmentWinsOverCancellation(t *testing.T) {
	h, cluster := setupDeliveryHandoff(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.request = func(context.Context, uint64, string, []byte) (*proto.Response, error) {
		cancel()
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	e := testPersistEvent("accepted", wkproto.ReasonSuccess, false)
	e.Type = eventbus.EventChannelDistributeInitial
	if err := h.attemptInitialDistribution(ctx, "chat", wkproto.ChannelTypeData, []*eventbus.Event{e}); err != nil {
		t.Fatalf("明确受理确认后不能重复交接：%v", err)
	}
}

type initialFanoutCluster struct {
	deliveryHandoffCluster
	sends   atomic.Int32
	badType atomic.Bool
}

func (c *initialFanoutCluster) Send(node uint64, msg *proto.Message) error {
	if node != 2 {
		return fmt.Errorf("错误的节点：%d", node)
	}
	req := &forwardChannelEventReq{}
	if err := req.decode(msg.Content); err != nil {
		return err
	}
	for _, event := range req.events {
		if event.Type != eventbus.EventChannelDistribute {
			c.badType.Store(true)
		}
	}
	c.sends.Add(1)
	return nil
}

func setupInitialFanout(t *testing.T, local bool) (*Handler, *initialFanoutCluster, *initialTagManager, *distributePusher) {
	t.Helper()
	h, users, pusher := setupDistributeDelivery(t)
	cluster := &initialFanoutCluster{}
	cluster.leader.Store(1)
	service.Cluster = cluster
	service.ConnRecovery = nil
	tag := &types.Tag{Key: "initial", Nodes: []*types.Node{{LeaderId: 2, Uids: []string{"remote"}}}}
	if local {
		tag.Nodes = append(tag.Nodes, &types.Node{LeaderId: 1, Uids: []string{"local"}})
		users.set("local", distributeConn("local", 1, 1, "local"))
	}
	manager := &initialTagManager{tag: tag}
	oldTags := service.TagManager
	service.TagManager = manager
	t.Cleanup(func() { h.Stop(); service.TagManager = oldTags })
	return h, cluster, manager, pusher
}

func TestInitialDistributionFansOutOnceWithAndWithoutLocalRecipients(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprint(local), func(t *testing.T) {
			h, cluster, manager, pusher := setupInitialFanout(t, local)
			var attempts atomic.Int32
			manager.before = func() error {
				if attempts.Add(1) == 1 {
					return errors.New("完整标签暂不可用")
				}
				return nil
			}
			e := testPersistEvent("initial", wkproto.ReasonSuccess, false)
			e.Type = eventbus.EventChannelDistributeInitial
			if err := h.enqueueInitialDistribution("chat", wkproto.ChannelTypeData, []*eventbus.Event{e}); err != nil {
				t.Fatal(err)
			}
			waitDistributeDeliveryEmpty(t, h)
			wantPush := 0
			if local {
				wantPush = 1
			}
			if attempts.Load() != 2 || cluster.sends.Load() != 1 || cluster.badType.Load() || len(pusher.snapshot()) != wantPush {
				t.Fatalf("重试全局分发错误：尝试=%d 转发=%d 类型错误=%v 推送=%d", attempts.Load(), cluster.sends.Load(), cluster.badType.Load(), len(pusher.snapshot()))
			}
			if e.Type != eventbus.EventChannelDistributeInitial || e.TagKey != "" {
				t.Fatal("不得修改初始模板")
			}
		})
	}
}

func TestInitialDistributionDoesNotPartiallyDeliverBeforeHandoff(t *testing.T) {
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprint(local), func(t *testing.T) {
			h, cluster, manager, pusher := setupInitialFanout(t, local)
			// 精确控制创建标签期间换主，不以 sleep 碰运气触发。
			manager.before = func() error { cluster.leader.Store(3); return nil }
			var handoffs atomic.Int32
			cluster.request = func(_ context.Context, node uint64, _ string, _ []byte) (*proto.Response, error) {
				if node != 3 {
					return nil, errors.New("未跟随新领导")
				}
				handoffs.Add(1)
				return &proto.Response{Status: proto.StatusOK}, nil
			}
			e := testPersistEvent("changed", wkproto.ReasonSuccess, false)
			e.Type = eventbus.EventChannelDistributeInitial
			if err := h.enqueueInitialDistribution("chat", wkproto.ChannelTypeData, []*eventbus.Event{e}); err != nil {
				t.Fatal(err)
			}
			waitDistributeDeliveryEmpty(t, h)
			if handoffs.Load() != 1 || cluster.sends.Load() != 0 || len(pusher.snapshot()) != 0 {
				t.Fatalf("交接前不能先投部分用户：交接=%d 整组转发=%d 推送=%d", handoffs.Load(), cluster.sends.Load(), len(pusher.snapshot()))
			}
		})
	}
}

func TestAcceptInitialDistributionValidatesOwnershipAndStage(t *testing.T) {
	for _, name := range []string{"非当前领导", "已停止", "空频道", "无消息", "在线命令", "错误阶段", "定向接收者", "无连接", "无帧"} {
		t.Run(name, func(t *testing.T) {
			h, cluster := setupDeliveryHandoff(t)
			h.SetRoutes()
			if cluster.routes[initialDistributionPath] == nil {
				t.Fatal("未注册首分发确认路由")
			}
			cluster.leader.Store(1)
			e := testPersistEvent("invalid", wkproto.ReasonSuccess, false)
			e.Type = eventbus.EventChannelDistributeInitial
			req := &forwardChannelEventReq{channelId: "chat", channelType: wkproto.ChannelTypeData, events: []*eventbus.Event{e}}
			switch name {
			case "非当前领导":
				cluster.leader.Store(2)
			case "已停止":
				h.Stop()
			case "空频道":
				req.channelId = ""
			case "无消息":
				req.events = nil
			case "在线命令":
				req.channelId = options.G.Channel.OnlineCmdChannelId
			case "错误阶段":
				e.Type = eventbus.EventChannelOnSend
			case "定向接收者":
				e.ToUid = "receiver"
			case "无连接":
				e.Conn = nil
			case "无帧":
				e.Frame = nil
			}
			body, err := req.encode()
			if err != nil {
				t.Fatal(err)
			}
			if err := h.acceptInitialDistribution(body); err == nil {
				t.Fatal("无效请求不应确认接收")
			}
			if h.deliveryQueue != nil {
				t.Fatal("无效请求不应产生待办或递归转发")
			}
		})
	}
}

func TestAcceptInitialDistributionAllowsZeroSequenceSendAndEvent(t *testing.T) {
	for _, eventFrame := range []bool{false, true} {
		t.Run(fmt.Sprint(eventFrame), func(t *testing.T) {
			h, cluster, _, _ := setupInitialFanout(t, false)
			e := testPersistEvent("ephemeral", 0, true)
			e.Type = eventbus.EventChannelDistributeInitial
			if eventFrame {
				e.Frame = &wkproto.EventPacket{Type: "test", Id: "1"}
			}
			req := &forwardChannelEventReq{channelId: "chat", channelType: wkproto.ChannelTypeData, events: []*eventbus.Event{e}}
			body, err := req.encode()
			if err != nil {
				t.Fatal(err)
			}
			if err := h.acceptInitialDistribution(body); err != nil {
				t.Fatal(err)
			}
			waitDistributeDeliveryEmpty(t, h)
			if cluster.sends.Load() != 1 || cluster.badType.Load() {
				t.Fatal("受理后必须只生成局部分发")
			}
		})
	}
}
