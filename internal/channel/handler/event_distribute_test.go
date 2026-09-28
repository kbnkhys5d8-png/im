package handler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type distributeRecovery func(context.Context, string) error

func (f distributeRecovery) Ensure(ctx context.Context, uid string) error { return f(ctx, uid) }

type distributeUsers struct {
	eventbus.IUser
	mu    sync.Mutex
	conns map[string][]*eventbus.Conn
}

func (u *distributeUsers) AuthedConnsByUid(uid string) []*eventbus.Conn {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]*eventbus.Conn(nil), u.conns[uid]...)
}
func (u *distributeUsers) set(uid string, conns ...*eventbus.Conn) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.conns[uid] = append([]*eventbus.Conn(nil), conns...)
}
func distributeConn(uid string, node uint64, id int64, session string) *eventbus.Conn {
	return &eventbus.Conn{Uid: uid, NodeId: node, ConnId: id, SessionId: session, Auth: true, DeviceLevel: wkproto.DeviceLevelMaster}
}

type distributePusher struct {
	mu     sync.Mutex
	events []*eventbus.Event
}

func (p *distributePusher) AddEvent(_ int, e *eventbus.Event) {
	if e.TakeOfflineEvents != nil {
		// 模拟推送器消费时展开聚合离线事件；回调不能在 mock 锁内执行。
		for _, event := range e.TakeOfflineEvents() {
			p.AddEvent(0, event)
		}
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, e)
}
func (p *distributePusher) RandHandlerId() int { return 0 }
func (p *distributePusher) Advance(int)        {}
func (p *distributePusher) snapshot() []*eventbus.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*eventbus.Event(nil), p.events...)
}

type distributeCluster struct{ icluster.ICluster }

func (distributeCluster) SlotLeaderIdOfChannel(_ string, channelType uint8) (uint64, error) {
	if channelType != wkproto.ChannelTypePerson {
		return 0, errors.New("接收者目录必须按个人频道查询领导")
	}
	return 1, nil
}

type distributeForwardCluster struct {
	distributeCluster
	calls atomic.Int64
}

func (c *distributeForwardCluster) Send(node uint64, _ *proto.Message) error {
	if node != 2 {
		return fmt.Errorf("整组转发的目标错误：%d", node)
	}
	c.calls.Add(1)
	return nil
}

type distributeConversations struct {
	service.IConversationManager
	calls atomic.Int64
}

func (c *distributeConversations) Push(string, uint8, string, []*eventbus.Event) {
	c.calls.Add(1)
}

func setupDistributeDelivery(t *testing.T) (*Handler, *distributeUsers, *distributePusher) {
	t.Helper()
	oldOptions, oldUser, oldPusher, oldRecovery, oldCluster, oldConversation := options.G, eventbus.User, eventbus.Pusher, service.ConnRecovery, service.Cluster, service.ConversationManager
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	options.G.Channel.ProcessTimeout = time.Second
	users := &distributeUsers{conns: make(map[string][]*eventbus.Conn)}
	pusher := &distributePusher{}
	eventbus.RegisterUser(users)
	eventbus.RegisterPusher(pusher)
	service.Cluster = distributeCluster{}
	h := &Handler{Log: wklog.NewWKLog("distribute-delivery-test")}
	t.Cleanup(func() {
		// 后台补投停止并等待在途任务退出后，才能恢复共享服务。
		h.Stop()
		options.G, eventbus.User, eventbus.Pusher, service.ConnRecovery, service.Cluster = oldOptions, oldUser, oldPusher, oldRecovery, oldCluster
		service.ConversationManager = oldConversation
	})
	return h, users, pusher
}
func waitDistributeDelivery(t *testing.T, condition func() bool, name string) {
	t.Helper()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for !condition() {
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatalf("等待%s超时", name)
		}
	}
}
func waitDistributeRecoverySignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("等待%s超时", name)
	}
}
func waitDistributeDeliveryEmpty(t *testing.T, h *Handler) {
	t.Helper()
	waitDistributeDelivery(t, func() bool { return h.deliveryQueue != nil && h.deliveryQueue.pendingCount() == 0 }, "异步投递全部完成")
}
func distributePushCounts(p *distributePusher) (online, offline int) {
	for _, e := range p.snapshot() {
		switch e.Type {
		case eventbus.EventPushOnline:
			online++
		case eventbus.EventPushOffline:
			offline++
		}
	}
	return
}

func TestDistributeRecoversBeforeOnlineClassification(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		recover, existing, failed, noRecovery bool
		wantOnline, wantOffline               int
	}{
		{name: "首条消息等待恢复", recover: true, wantOnline: 1},
		{name: "确实离线仍走原流程", wantOffline: 1},
		{name: "恢复失败不能确认离线", failed: true},
		{name: "失败仍投递已有连接", existing: true, failed: true, wantOnline: 1},
		{name: "无恢复服务保留原推送", existing: true, noRecovery: true, wantOnline: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, users, pusher := setupDistributeDelivery(t)
			addConn := func() { users.set("receiver", distributeConn("receiver", 1, 1, "session")) }
			if tc.existing {
				addConn()
			}
			var calls atomic.Int64
			service.ConnRecovery = distributeRecovery(func(ctx context.Context, uid string) error {
				calls.Add(1)
				if uid != "receiver" {
					t.Errorf("不应恢复其他用户 %q", uid)
					return errors.New("错误用户")
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("每次恢复必须有独立超时")
				}
				if tc.failed {
					return errors.New("owner unavailable")
				}
				if tc.recover {
					addConn()
				}
				return nil
			})
			if tc.noRecovery {
				service.ConnRecovery = nil
			}
			tag := &types.Tag{Nodes: []*types.Node{{LeaderId: 1, Uids: []string{"receiver"}}}}
			// 数据频道跳过会话更新，只验证分类及补投流程。
			h.distributeByTag(1, tag, "channel", wkproto.ChannelTypeData, []*eventbus.Event{testPersistEvent("first", wkproto.ReasonSuccess, false)})
			if tc.failed {
				waitDistributeDelivery(t, func() bool { return calls.Load() >= 2 }, "失败任务再次尝试")
				if h.deliveryQueue.pendingCount() != 1 {
					t.Fatal("失败任务必须留在内存队列，不能判为已完成")
				}
			} else {
				waitDistributeDeliveryEmpty(t, h)
			}
			online, offline := distributePushCounts(pusher)
			if online != tc.wantOnline || offline != tc.wantOffline {
				t.Fatalf("恢复次数=%d，在线推送=%d，离线推送=%d", calls.Load(), online, offline)
			}
			if tc.noRecovery && calls.Load() != 0 || !tc.noRecovery && !tc.failed && calls.Load() != 1 {
				t.Fatalf("恢复次数不符合预期：%d", calls.Load())
			}
		})
	}
}

func TestDistributeRetryOnlyPushesPreviouslyUnseenSessions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		node    uint64
		id      int64
		replace bool
	}{
		{name: "恢复另一台设备", node: 2, id: 2},
		{name: "同连接编号已换代", node: 1, id: 1, replace: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, users, pusher := setupDistributeDelivery(t)
			known := distributeConn("receiver", 1, 1, "known")
			users.set("receiver", known)
			entered, release := make(chan struct{}), make(chan struct{})
			var enteredOnce sync.Once
			var calls atomic.Int64
			service.ConnRecovery = distributeRecovery(func(ctx context.Context, uid string) error {
				if calls.Add(1) == 1 {
					return errors.New("首次恢复失败")
				}
				enteredOnce.Do(func() { close(entered) })
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			if err := h.enqueueDelivery("channel", wkproto.ChannelTypeData, "receiver", []*eventbus.Event{testPersistEvent("retry", wkproto.ReasonSuccess, false)}); err != nil {
				t.Fatal(err)
			}
			waitDistributeRecoverySignal(t, entered, "第二次恢复进入")
			if online, offline := distributePushCounts(pusher); online != 1 || offline != 0 {
				t.Fatalf("首次失败应只发已有设备：online=%d offline=%d", online, offline)
			}
			newConn := distributeConn("receiver", tc.node, tc.id, "new")
			if tc.replace {
				users.set("receiver", newConn)
			} else {
				users.set("receiver", known, newConn)
			}
			close(release)
			waitDistributeDeliveryEmpty(t, h)
			var sessions []string
			for _, event := range pusher.snapshot() {
				if event.Type != eventbus.EventPushOnline || len(event.ToConns) != 1 {
					t.Fatalf("每次只可定向发送尚未投递的会话：%+v", event)
				}
				sessions = append(sessions, event.ToConns[0].SessionId)
			}
			if !reflect.DeepEqual(sessions, []string{"known", "new"}) || calls.Load() != 2 {
				t.Fatalf("补投会话=%v，恢复次数=%d", sessions, calls.Load())
			}
		})
	}
}

func TestDistributeRecoveryTimeoutStillPushesKnownSession(t *testing.T) {
	h, users, pusher := setupDistributeDelivery(t)
	options.G.Channel.ProcessTimeout = 20 * time.Millisecond
	users.set("receiver", distributeConn("receiver", 1, 1, "known"))
	var calls atomic.Int64
	service.ConnRecovery = distributeRecovery(func(ctx context.Context, _ string) error {
		calls.Add(1)
		<-ctx.Done()
		return ctx.Err()
	})
	if err := h.enqueueDelivery("channel", wkproto.ChannelTypeData, "receiver", []*eventbus.Event{testPersistEvent("timeout", wkproto.ReasonSuccess, false)}); err != nil {
		t.Fatal(err)
	}
	// 第二轮已进入说明首轮超时的投递分类已经执行，不靠短暂 sleep 猜测顺序。
	waitDistributeDelivery(t, func() bool { return calls.Load() >= 2 }, "超时后保留任务并再次恢复")
	if online, offline := distributePushCounts(pusher); online != 1 || offline != 0 {
		t.Fatalf("恢复超时仍需投递已知会话且不能判离线：online=%d offline=%d", online, offline)
	}
	if h.deliveryQueue.pendingCount() != 1 {
		t.Fatal("已知会话投递完成不能丢弃仍未恢复的接收者待办")
	}
}

func TestDistributeRetriesDoNotRepeatConversationOrFullTagForward(t *testing.T) {
	h, users, pusher := setupDistributeDelivery(t)
	cluster, conversations := &distributeForwardCluster{}, &distributeConversations{}
	service.Cluster, service.ConversationManager = cluster, conversations
	users.set("receiver", distributeConn("receiver", 1, 1, "known"))
	var calls atomic.Int64
	service.ConnRecovery = distributeRecovery(func(context.Context, string) error {
		if calls.Add(1) == 1 {
			return errors.New("首轮目录恢复失败")
		}
		return nil
	})
	tag := &types.Tag{Key: "original-tag", Nodes: []*types.Node{
		{LeaderId: 1, Uids: []string{"receiver"}},
		{LeaderId: 2, Uids: []string{"remote-receiver"}},
	}}
	h.distributeByTag(1, tag, "group", wkproto.ChannelTypeGroup, []*eventbus.Event{testPersistEvent("once", wkproto.ReasonSuccess, false)})
	waitDistributeDeliveryEmpty(t, h)
	if calls.Load() != 2 || cluster.calls.Load() != 1 || conversations.calls.Load() != 1 {
		t.Fatalf("恢复=%d，完整标签转发=%d，会话更新=%d；补投不能重复入口副作用", calls.Load(), cluster.calls.Load(), conversations.calls.Load())
	}
	if online, offline := distributePushCounts(pusher); online != 1 || offline != 0 {
		t.Fatalf("已发会话不能因重试重复推送：online=%d offline=%d", online, offline)
	}
}

func TestDistributeSlowUIDDoesNotBlockHealthyAndPreservesRecipientFIFO(t *testing.T) {
	h, users, pusher := setupDistributeDelivery(t)
	users.set("slow", distributeConn("slow", 1, 1, "slow-session"))
	users.set("healthy", distributeConn("healthy", 1, 2, "healthy-session"))
	entered, release := make(chan struct{}), make(chan struct{})
	var slowCalls atomic.Int64
	service.ConnRecovery = distributeRecovery(func(ctx context.Context, uid string) error {
		if uid == "slow" && slowCalls.Add(1) == 1 {
			close(entered)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	first, second := testPersistEvent("first", wkproto.ReasonSuccess, false), testPersistEvent("second", wkproto.ReasonSuccess, false)
	first.MessageId, second.MessageId = 101, 102
	if err := h.enqueueDelivery("channel", wkproto.ChannelTypeData, "slow", []*eventbus.Event{first}); err != nil {
		t.Fatal(err)
	}
	waitDistributeRecoverySignal(t, entered, "慢用户开始恢复")
	if err := h.enqueueDelivery("channel", wkproto.ChannelTypeData, "slow", []*eventbus.Event{second}); err != nil {
		t.Fatal(err)
	}
	if err := h.enqueueDelivery("channel", wkproto.ChannelTypeData, "healthy", []*eventbus.Event{first}); err != nil {
		t.Fatal(err)
	}
	waitDistributeDelivery(t, func() bool {
		for _, event := range pusher.snapshot() {
			if event.ToUid == "healthy" && event.Type == eventbus.EventPushOnline {
				return true
			}
		}
		return false
	}, "慢用户未放行时健康用户已经完成")
	if slowCalls.Load() != 1 {
		t.Fatal("同一接收者的后续消息不能越过正在处理的队首")
	}
	close(release)
	waitDistributeDeliveryEmpty(t, h)
	var slowMessages []int64
	for _, event := range pusher.snapshot() {
		if event.Type == eventbus.EventPushOffline {
			t.Fatal("在线设备不能被判为离线")
		}
		if event.ToUid == "slow" {
			slowMessages = append(slowMessages, event.MessageId)
		}
	}
	if !reflect.DeepEqual(slowMessages, []int64{101, 102}) {
		t.Fatalf("同接收者消息顺序=%v", slowMessages)
	}
}

func TestDistributeFiveThousandUIDsReceiveAcrossIndependentDeadlineWaves(t *testing.T) {
	h, users, pusher := setupDistributeDelivery(t)
	const count = 5000
	options.G.Channel.ProcessTimeout = 250 * time.Millisecond
	uids := make([]string, count)
	for i := range uids {
		uids[i] = fmt.Sprintf("user-%d", i)
		users.set(uids[i], distributeConn(uids[i], 1, int64(i+1), "session"))
	}
	var active, peak, expiredAtEntry, calls atomic.Int64
	service.ConnRecovery = distributeRecovery(func(ctx context.Context, _ string) error {
		calls.Add(1)
		now := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); now > old && !peak.CompareAndSwap(old, now); old = peak.Load() {
		}
		if _, ok := ctx.Deadline(); !ok || ctx.Err() != nil {
			expiredAtEntry.Add(1)
		}
		// 总恢复时间刻意超过单次超时，后续波次必须重新获得期限。
		timer := time.NewTimer(4 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	start := time.Now()
	tag := &types.Tag{Nodes: []*types.Node{{LeaderId: 1, Uids: uids}}}
	h.distributeByTag(1, tag, "large-group", wkproto.ChannelTypeData, []*eventbus.Event{testPersistEvent("all-users", wkproto.ReasonSuccess, false)})
	waitDistributeDeliveryEmpty(t, h)
	seen := make(map[string]int, count)
	for _, event := range pusher.snapshot() {
		if event.Type != eventbus.EventPushOnline || len(event.ToConns) != 1 {
			t.Fatalf("健康用户应定向收到在线消息：type=%v uid=%s targets=%d", event.Type, event.ToUid, len(event.ToConns))
		}
		seen[event.ToUid]++
	}
	for _, uid := range uids {
		if seen[uid] != 1 {
			t.Fatalf("用户%s投递次数=%d，不能遗漏或重复", uid, seen[uid])
		}
	}
	if elapsed := time.Since(start); elapsed <= options.G.Channel.ProcessTimeout {
		t.Fatalf("未跨越原共享deadline，测试不能说明排队仍能完成：%s", elapsed)
	}
	if expiredAtEntry.Load() != 0 || peak.Load() > 16 || active.Load() != 0 || calls.Load() < count {
		t.Fatalf("开始即过期=%d，并发峰值=%d，残留=%d，恢复次数=%d", expiredAtEntry.Load(), peak.Load(), active.Load(), calls.Load())
	}
}

func TestDistributeSkipsSystemUIDAndStopCancelsPending(t *testing.T) {
	h, _, pusher := setupDistributeDelivery(t)
	entered, exited := make(chan struct{}), make(chan struct{})
	var enteredOnce, exitedOnce sync.Once
	var calls atomic.Int64
	service.ConnRecovery = distributeRecovery(func(ctx context.Context, uid string) error {
		calls.Add(1)
		if options.G.IsSystemUid(uid) {
			t.Error("系统用户不能调用连接恢复")
			return nil
		}
		enteredOnce.Do(func() { close(entered) })
		<-ctx.Done()
		exitedOnce.Do(func() { close(exited) })
		return ctx.Err()
	})
	tag := &types.Tag{Nodes: []*types.Node{{LeaderId: 1, Uids: []string{options.G.SystemUID, "unavailable"}}}}
	h.distributeByTag(1, tag, "channel", wkproto.ChannelTypeData, []*eventbus.Event{testPersistEvent("pending", wkproto.ReasonSuccess, false)})
	waitDistributeRecoverySignal(t, entered, "普通用户恢复进入")
	h.Stop()
	waitDistributeRecoverySignal(t, exited, "停止取消在途恢复")
	if calls.Load() != 1 {
		t.Fatalf("系统用户不应恢复，实际调用%d次", calls.Load())
	}
	if online, offline := distributePushCounts(pusher); online != 0 || offline != 0 {
		t.Fatalf("停止或失败不能制造离线事件：online=%d offline=%d", online, offline)
	}
	if err := h.enqueueDelivery("channel", wkproto.ChannelTypeData, "later", []*eventbus.Event{testPersistEvent("later", wkproto.ReasonSuccess, false)}); err == nil {
		t.Fatal("停止后不能接受新任务")
	}
}
