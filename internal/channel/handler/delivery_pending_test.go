package handler

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type deliveryHandoffCluster struct {
	icluster.ICluster
	leader  atomic.Uint64
	request func(context.Context, uint64, string, []byte) (*proto.Response, error)
	routes  map[string]wkserver.Handler
}

func (c *deliveryHandoffCluster) SlotLeaderIdOfChannel(string, uint8) (uint64, error) {
	return c.leader.Load(), nil
}

func (c *deliveryHandoffCluster) RequestWithContext(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
	return c.request(ctx, node, path, body)
}

func (c *deliveryHandoffCluster) Route(path string, h wkserver.Handler) { c.routes[path] = h }

func setupDeliveryHandoff(t *testing.T) (*Handler, *deliveryHandoffCluster) {
	t.Helper()
	oldOptions, oldCluster := options.G, service.Cluster
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	cluster := &deliveryHandoffCluster{routes: make(map[string]wkserver.Handler)}
	cluster.leader.Store(2)
	service.Cluster = cluster
	h := &Handler{Log: wklog.NewWKLog("delivery-handoff-test")}
	t.Cleanup(func() {
		h.Stop()
		options.G, service.Cluster = oldOptions, oldCluster
	})
	return h, cluster
}

func waitDeliveryHandoff(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !done() {
		select {
		case <-deadline.C:
			t.Fatal("等待补投状态超时")
		case <-tick.C:
		}
	}
}

func TestDeliveryHandoffRequiresAcknowledgmentAndCanReturnToSource(t *testing.T) {
	h, cluster := setupDeliveryHandoff(t)
	var calls atomic.Int32
	bodies := make(chan *forwardChannelEventReq, 2)
	cluster.request = func(ctx context.Context, node uint64, path string, body []byte) (*proto.Response, error) {
		if node != 2 || path != pendingDeliveryPath {
			return nil, errors.New("错误的补投节点或路径")
		}
		if _, ok := ctx.Deadline(); !ok {
			return nil, errors.New("补投请求没有独立超时")
		}
		req := &forwardChannelEventReq{}
		if err := req.decode(body); err != nil {
			return nil, err
		}
		bodies <- req
		if calls.Add(1) == 1 {
			return &proto.Response{Status: proto.StatusError}, nil
		}
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	e := testPersistEvent("handoff", wkproto.ReasonSuccess, false)
	e.SourceNodeId = 2
	if err := h.enqueueDelivery("channel", wkproto.ChannelTypeData, "receiver", []*eventbus.Event{e}); err != nil {
		t.Fatal(err)
	}
	waitDeliveryHandoff(t, func() bool { return h.deliveryQueue.pendingCount() == 0 })
	if got := calls.Load(); got != 2 {
		t.Fatalf("必须在收到成功确认后才移除待办，调用=%d", got)
	}
	for range 2 {
		req := <-bodies
		if req.channelId != "channel" || req.channelType != wkproto.ChannelTypeData ||
			len(req.events) != 1 || req.events[0].ToUid != "receiver" ||
			req.events[0].Type != eventbus.EventChannelDistribute || req.events[0].SourceNodeId != 2 {
			t.Fatalf("定向补投不能退化为整组转发：%+v", req)
		}
	}
	if e.ToUid != "" {
		t.Fatal("移交不应修改共享消息模板")
	}
}

func TestDeliveryHandoffStopCancelsRequest(t *testing.T) {
	h, cluster := setupDeliveryHandoff(t)
	entered := make(chan struct{})
	canceled := make(chan struct{})
	cluster.request = func(ctx context.Context, _ uint64, _ string, _ []byte) (*proto.Response, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}
	events := []*eventbus.Event{testPersistEvent("handoff", wkproto.ReasonSuccess, false)}
	if err := h.enqueueDelivery("channel", wkproto.ChannelTypeData, "receiver", events); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("移交没有开始")
	}
	h.Stop()
	select {
	case <-canceled:
	default:
		t.Fatal("停止返回时移交请求必须已经退出")
	}
	if err := h.enqueueDelivery("channel", wkproto.ChannelTypeData, "receiver", events); !errors.Is(err, errDeliveryQueueStopped) {
		t.Fatalf("停止后不能重新入队：%v", err)
	}
}

func TestDeliveryHandoffAcknowledgmentWinsOverConcurrentCancellation(t *testing.T) {
	h, cluster := setupDeliveryHandoff(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cluster.request = func(context.Context, uint64, string, []byte) (*proto.Response, error) {
		cancel()
		return &proto.Response{Status: proto.StatusOK}, nil
	}
	events := []*eventbus.Event{testPersistEvent("accepted", wkproto.ReasonSuccess, false)}
	if err := h.handoffDelivery(ctx, 2, "channel", wkproto.ChannelTypeData, "receiver", events); err != nil {
		t.Fatalf("已经确认受理后不能因并发取消重复移交：%v", err)
	}
}

func TestAcceptPendingDeliveryQueuesOnlyTheNamedReceiver(t *testing.T) {
	h, users, pusher := setupDistributeDelivery(t)
	users.set("receiver", distributeConn("receiver", 1, 1, "session"))
	var calls atomic.Int32
	service.ConnRecovery = distributeRecovery(func(_ context.Context, uid string) error {
		if uid != "receiver" {
			t.Errorf("定向补投不能扩散到 %q", uid)
		}
		calls.Add(1)
		return nil
	})
	// 不提供 tag 或会话管理器；受理必须直接入单用户队列。
	service.ConversationManager = nil
	e := testPersistEvent("pending", wkproto.ReasonSuccess, false)
	e.Type, e.ToUid = eventbus.EventChannelDistribute, "receiver"
	req := &forwardChannelEventReq{channelId: "channel", channelType: wkproto.ChannelTypeGroup, events: []*eventbus.Event{e}}
	body, err := req.encode()
	if err != nil {
		t.Fatal(err)
	}
	if err = h.acceptPendingDelivery(body); err != nil {
		t.Fatal(err)
	}
	waitDistributeDeliveryEmpty(t, h)
	events := pusher.snapshot()
	if calls.Load() != 1 || len(events) != 1 || events[0].ToUid != "receiver" || events[0].ChannelType != wkproto.ChannelTypeGroup {
		t.Fatalf("定向受理结果错误：恢复次数=%d，推送=%+v", calls.Load(), events)
	}
}

func TestAcceptPendingDeliveryRejectsWholeTagAndStaleLeader(t *testing.T) {
	h, cluster := setupDeliveryHandoff(t)
	h.SetRoutes()
	if cluster.routes[pendingDeliveryPath] == nil {
		t.Fatal("未注册集群内补投路由")
	}
	for _, name := range []string{"无消息", "缺接收者", "不同接收者", "不完整事件", "错误类型", "非当前领导", "已停止"} {
		t.Run(name, func(t *testing.T) {
			cluster.leader.Store(1)
			e := testPersistEvent("pending", wkproto.ReasonSuccess, false)
			e.Type, e.ToUid = eventbus.EventChannelDistribute, "receiver"
			req := &forwardChannelEventReq{channelId: "channel", channelType: wkproto.ChannelTypeData, events: []*eventbus.Event{e}}
			switch name {
			case "无消息":
				req.events = nil
			case "缺接收者":
				e.ToUid = ""
			case "不同接收者":
				other := e.Clone()
				other.ToUid = "other"
				req.events = append(req.events, other)
			case "不完整事件":
				e.Conn = nil
			case "错误类型":
				e.Type = eventbus.EventChannelOnSend
			case "非当前领导":
				cluster.leader.Store(2)
			case "已停止":
				h.Stop()
			}
			body, err := req.encode()
			if err != nil {
				t.Fatal(err)
			}
			if err = h.acceptPendingDelivery(body); err == nil {
				t.Fatal("无效或陈旧请求不应收到受理确认")
			}
			if h.deliveryQueue != nil {
				t.Fatal("拒绝请求不应创建任务或重新整组分发")
			}
		})
	}
}
