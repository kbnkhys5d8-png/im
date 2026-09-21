package webhook

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/grpcpool"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhook"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/nsqio/go-diskqueue"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type Webhook struct {
	wklog.Log
	eventPool        *ants.Pool
	httpClient       *http.Client
	webhookGRPCPool  *grpcpool.Pool // webhook grpc客户端
	stoped           chan struct{}
	onlinestatusLock sync.RWMutex
	onlinestatusList []string
	focusEvents      map[string]struct{} // 用户关注的事件类型,如果为空则推送所有类型
	backend          diskqueue.Interface
	failures         notifyFailureStore
	requestCtx       context.Context
	cancelRequests   context.CancelFunc
	workers          sync.WaitGroup
	stopOnce         sync.Once
	stopErr          error
}

func New() *Webhook {
	eventPool, err := ants.NewPool(options.G.EventPoolSize, ants.WithPanicHandler(func(err interface{}) {
		wklog.Panic("Webhook panic", zap.Any("err", err), zap.Stack("stack"))
	}))
	if err != nil {
		panic(err)
	}
	var (
		webhookGRPCPool *grpcpool.Pool
	)
	if options.G.WebhookGRPCOn() {
		webhookGRPCPool, err = grpcpool.New(func() (*grpc.ClientConn, error) {
			return grpc.Dial(options.G.Webhook.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    5 * time.Minute, // send pings every 5 minute if there is no activity
				Timeout: 2 * time.Second, // wait 1 second for ping ack before considering the connection dead
			}))
		}, 2, 20, time.Minute*5) // 初始化2个连接 最多20个连接
		if err != nil {
			panic(err)
		}

	}

	// 检查用户配置了关注的事件
	var focusEvents = make(map[string]struct{})
	if len(options.G.Webhook.FocusEvents) > 0 {
		for _, focusEvent := range options.G.Webhook.FocusEvents {
			if focusEvent == "" {
				continue
			}
			if _, ok := eventWebHook[focusEvent]; ok {
				focusEvents[focusEvent] = struct{}{}
			}
		}
	}

	minMsgSize := int32(2)
	maxMsgSize := int32(1024 * 1024 * 200)
	dataDir := path.Join(options.G.DataDir, "diskqueue")
	maxBytesPerFile := int64(1024 * 1024 * 1024)
	syncEvery := int64(2500)       // 每2500次写入同步一次
	syncTimeout := time.Second * 2 // 每次间隔2秒同步一次
	err = os.MkdirAll(dataDir, 0755)
	if err != nil {
		panic(err)
	}
	failures, err := newNotifyFailureStore(path.Join(options.G.DataDir, "webhook_failed"))
	if err != nil {
		panic(err)
	}
	requestCtx, cancelRequests := context.WithCancel(context.Background())

	backend := diskqueue.New(
		"wk_webhook_q",
		dataDir,
		maxBytesPerFile,
		minMsgSize,
		maxMsgSize,
		syncEvery,
		syncTimeout,
		func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
			wklog.Info("webhook backend", zap.String("lvl", lvl.String()), zap.String("f", f), zap.Any("args", args))
		},
	)

	return &Webhook{
		Log:              wklog.NewWKLog("Webhook"),
		eventPool:        eventPool,
		webhookGRPCPool:  webhookGRPCPool,
		onlinestatusList: make([]string, 0),
		stoped:           make(chan struct{}),
		backend:          backend,
		failures:         failures,
		requestCtx:       requestCtx,
		cancelRequests:   cancelRequests,
		httpClient: &http.Client{
			Transport: &http.Transport{
				DialContext: (&net.Dialer{
					Timeout:   5 * time.Second,
					KeepAlive: 30 * time.Second, // 增加KeepAlive时间
				}).DialContext,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          500,              // 增加最大空闲连接数
				MaxIdleConnsPerHost:   100,              // 增加每个主机的最大空闲连接数
				MaxConnsPerHost:       100,              // 设置每个主机的最大连接数
				IdleConnTimeout:       90 * time.Second, // 减少空闲连接超时
				TLSHandshakeTimeout:   10 * time.Second, // 增加TLS握手超时
				ResponseHeaderTimeout: 10 * time.Second, // 增加响应头超时
				ExpectContinueTimeout: 1 * time.Second,
				DisableCompression:    false, // 启用压缩
			},
			Timeout: 30 * time.Second, // 设置总体超时时间
		},
		focusEvents: focusEvents,
	}
}

func (w *Webhook) Start() error {
	err := w.recoverNotifyQueue()
	if err != nil {
		return err
	}
	w.workers.Add(2)
	go func() {
		defer w.workers.Done()
		w.notifyQueueLoop()
	}()
	go func() {
		defer w.workers.Done()
		w.loopOnlineStatus()
	}()

	return nil
}

// recoverNotifyQueue 恢复通知队列(兼容老版本)
func (w *Webhook) recoverNotifyQueue() error {
	messages, err := service.Store.GetMessagesOfNotifyQueue(4000)
	if err != nil {
		return err
	}
	if len(messages) == 0 {
		return nil
	}
	err = w.AppendMessageOfNotifyQueue(messages)
	if err != nil {
		return err
	}

	messageIDs := make([]int64, 0, len(messages))
	for _, msg := range messages {
		messageIDs = append(messageIDs, msg.MessageID)
	}
	err = service.Store.RemoveMessagesOfNotifyQueue(messageIDs)
	if err != nil {
		return err
	}

	return nil
}

func (w *Webhook) Stop() error {
	w.stopOnce.Do(func() {
		close(w.stoped)
		if w.cancelRequests != nil {
			w.cancelRequests()
		}
		// 先等消费协程保留未确认缓存，再关闭磁盘队列，停止时不额外补发。
		w.workers.Wait()
		if err := w.backend.Close(); err != nil && w.stopErr == nil {
			w.stopErr = err
		}
		w.httpClient.CloseIdleConnections()
	})
	return w.stopErr
}

// Online 用户设备上线通知
func (w *Webhook) Online(uid string, deviceFlag wkproto.DeviceFlag, connId int64, deviceOnlineCount int, totalOnlineCount int) {
	w.onlinestatusLock.Lock()
	defer w.onlinestatusLock.Unlock()
	online := 1
	w.onlinestatusList = append(w.onlinestatusList, fmt.Sprintf("%s-%d-%d-%d-%d-%d", uid, deviceFlag, online, connId, deviceOnlineCount, totalOnlineCount))

	w.Debug("User online", zap.String("uid", uid), zap.String("deviceFlag", deviceFlag.String()), zap.Int64("id", connId))
}

func (w *Webhook) Offline(uid string, deviceFlag wkproto.DeviceFlag, connId int64, deviceOnlineCount int, totalOnlineCount int) {
	w.onlinestatusLock.Lock()
	defer w.onlinestatusLock.Unlock()
	online := 0
	// 用户ID-用户设备标记-在线状态-socket ID-当前设备标记下的设备在线数量-当前用户下的所有设备在线数量
	w.onlinestatusList = append(w.onlinestatusList, fmt.Sprintf("%s-%d-%d-%d-%d-%d", uid, deviceFlag, online, connId, deviceOnlineCount, totalOnlineCount))

	w.Debug("User offline", zap.String("uid", uid), zap.String("deviceFlag", deviceFlag.String()))
}

// TriggerEvent 触发事件
func (w *Webhook) TriggerEvent(event *types.Event) {
	if !options.G.WebhookOn(event.Event) { // 没设置webhook直接忽略
		return
	}
	err := w.eventPool.Submit(func() {
		jsonData, err := json.Marshal(event.Data)
		if err != nil {
			w.Error("webhook的event数据不能json化！", zap.Error(err))
			return
		}

		if options.G.WebhookGRPCOn() {
			err = w.sendWebhookForGRPC(event.Event, jsonData)
		} else {
			err = w.sendWebhookForHttp(event.Event, jsonData)
		}
		if err != nil {
			w.Error("请求webhook失败！", zap.Error(err), zap.String("event", event.Event))
			return
		}

	})
	if err != nil {
		w.Error("提交事件失败", zap.Error(err))
	}
}

func (w *Webhook) NotifyOfflineMsg(msgs []*eventbus.Event) {
	for _, msg := range msgs {
		w.notifyOfflineMsg(msg, msg.OfflineUsers)
	}
}

func (w *Webhook) notifyOfflineMsg(e *eventbus.Event, subscribers []string) {
	switch frame := e.Frame.(type) {
	case *wkproto.SendPacket:
		w.pushOfflineMessages(e, frame, subscribers)
	case *wkproto.EventPacket:
		// EventPacket不需要处理离线消息
	}
}

func (w *Webhook) pushOfflineMessages(e *eventbus.Event, sendPacket *wkproto.SendPacket, subscribers []string) {
	compress := ""
	toUIDs := subscribers
	var compresssToUIDs []byte
	if options.G.Channel.SubscriberCompressOfCount > 0 && len(subscribers) > options.G.Channel.SubscriberCompressOfCount {
		buff := new(bytes.Buffer)
		gWriter := gzip.NewWriter(buff)
		defer gWriter.Close()
		_, err := gWriter.Write([]byte(wkutil.ToJSON(subscribers)))
		if err != nil {
			w.Error("压缩订阅者失败！", zap.Error(err))
		} else {
			toUIDs = make([]string, 0)
			compress = "gzip"
			compresssToUIDs = buff.Bytes()
		}
	}
	// 推送离线到上层应用
	w.TriggerEvent(&types.Event{
		Event: types.EventMsgOffline,
		Data: types.MessageOfflineNotify{
			MessageResp: types.MessageResp{
				Header: types.MessageHeader{
					RedDot:    wkutil.BoolToInt(sendPacket.RedDot),
					SyncOnce:  wkutil.BoolToInt(sendPacket.SyncOnce),
					NoPersist: wkutil.BoolToInt(sendPacket.NoPersist),
				},
				Setting:      sendPacket.Setting.Uint8(),
				ClientMsgNo:  sendPacket.ClientMsgNo,
				MessageId:    e.MessageId,
				MessageIdStr: strconv.FormatInt(e.MessageId, 10),
				MessageSeq:   e.MessageSeq,
				FromUID:      e.Conn.Uid,
				ChannelID:    sendPacket.ChannelID,
				ChannelType:  sendPacket.ChannelType,
				Topic:        sendPacket.Topic,
				Expire:       sendPacket.Expire,
				Timestamp:    int32(time.Now().Unix()),
				Payload:      sendPacket.Payload,
			},
			ToUids:          toUIDs,
			Compress:        compress,
			CompresssToUids: compresssToUIDs,
			SourceId:        int64(options.G.Cluster.NodeId),
		},
	})
}

func (w *Webhook) AppendMessageOfNotifyQueue(messages []wkdb.Message) error {
	for _, msg := range messages {
		if w.isStopping() {
			return errors.New("webhook is stopping")
		}
		data, err := msg.Marshal()
		if err != nil {
			return err
		}
		err = w.backend.Put(data)
		if err != nil {
			return err
		}
	}
	return nil
}

// 通知上层应用；失败记录转入人工核验目录，不通过截断缓存释放内存。
func (w *Webhook) notifyQueueLoop() {
	pushThreshold := options.G.Webhook.MsgNotifyEventCountPerPush
	if pushThreshold <= 0 {
		pushThreshold = 20
	}
	if !options.G.WebhookOn(types.EventMsgNotify) {
		return
	}
	interval := options.G.Webhook.MsgNotifyEventPushInterval
	if interval <= 0 {
		interval = time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	localMessageCache := make([]wkdb.Message, 0, pushThreshold)
	errMessageIDMap := make(map[int64]int)
	defer func() {
		if len(localMessageCache) == 0 {
			return
		}
		if err := w.failures.Save(localMessageCache, "shutdown_unconfirmed"); err != nil {
			w.stopErr = fmt.Errorf("保留停止时的未确认通知失败: %w", err)
			w.Error("未确认通知未能全部落盘，必须人工核验", zap.Error(err), zap.Int("count", len(localMessageCache)))
		}
	}()
	for {
		if w.isStopping() {
			return
		}
		// 内存最多保留一批；失败尚未处理完时暂停消费，不删除最旧消息。
		readChan := w.backend.ReadChan()
		if len(localMessageCache) >= pushThreshold || len(errMessageIDMap) > 0 {
			readChan = nil
		}
		var shouldPush bool
		select {
		case data, ok := <-readChan:
			if !ok {
				return
			}
			var msg wkdb.Message
			if err := msg.Unmarshal(data); err != nil {
				w.Error("Failed to unmarshal message from diskqueue", zap.Error(err), zap.Int("data_len", len(data)))
				continue
			}
			localMessageCache = append(localMessageCache, msg)
			shouldPush = len(localMessageCache) >= pushThreshold
		case <-ticker.C:
			shouldPush = len(localMessageCache) > 0
		case <-w.stoped:
			return
		}
		if shouldPush {
			var allSucceeded bool
			allSucceeded, localMessageCache = w.pushMessages(localMessageCache, errMessageIDMap)
			if !allSucceeded && !w.waitOrStop(time.Second) {
				return
			}
		}
	}
}

// pushMessages 是一个辅助函数，用于处理实际的消息推送逻辑
// 暂停人工核验不等于投递成功；只有远端确认才返回 allSucceeded=true。
// retryableMessages 也保留归档失败的消息，此时背压等待存储恢复，不继续请求远端。
func (w *Webhook) pushMessages(messages []wkdb.Message, errMessageIDMap map[int64]int) (allSucceeded bool, retryableMessages []wkdb.Message) {
	if len(messages) == 0 {
		return true, nil
	}
	failed := make([]wkdb.Message, 0, len(messages))
	for _, message := range messages {
		if errMessageIDMap[message.MessageID] > 0 {
			failed = append(failed, message)
		}
	}
	// 两种子批共用保留门禁，任一归档未完成时都不能先重发另一种消息。
	if len(failed) > 0 {
		if err := w.failures.Save(failed, "send_failed"); err != nil {
			w.Error("失败通知保留未完成，暂停发送并保留内存批次", zap.Error(err), zap.Int("count", len(failed)))
			return false, messages
		}
	}

	// 分组消息
	normalMessages, agentMessages := w.groupMessagesByType(messages)

	// 处理普通消息
	normalOK, normalRetryable, archiveErr := w.processBatchMessages(normalMessages, errMessageIDMap, "normal", w.sendNormalMessages)
	if archiveErr != nil {
		return false, append(normalRetryable, agentMessages...)
	}

	// 处理Agent消息
	agentOK, agentRetryable, _ := w.processBatchMessages(agentMessages, errMessageIDMap, "agent", w.sendAgentMessages)

	// 合并需要重试的消息
	retryableMessages = append(normalRetryable, agentRetryable...)

	return normalOK && agentOK, retryableMessages
}

// groupMessagesByType 将消息按类型分组
func (w *Webhook) groupMessagesByType(messages []wkdb.Message) (normalMessages, agentMessages []wkdb.Message) {
	if len(messages) == 0 {
		return nil, nil
	}

	// 第一次遍历：统计准确数量
	var agentCount int
	agentWebhookOn := options.G.AgentWebhookOn() // 缓存条件判断结果

	for i := range messages {
		if agentWebhookOn && messages[i].ChannelType == wkproto.ChannelTypeAgent {
			agentCount++
		}
	}

	normalCount := len(messages) - agentCount

	// 根据准确数量预分配数组
	if normalCount > 0 {
		normalMessages = make([]wkdb.Message, 0, normalCount)
	}
	if agentCount > 0 {
		agentMessages = make([]wkdb.Message, 0, agentCount)
	}

	// 第二次遍历：填充数组
	for i := range messages {
		if agentWebhookOn && messages[i].ChannelType == wkproto.ChannelTypeAgent {
			agentMessages = append(agentMessages, messages[i])
		} else {
			normalMessages = append(normalMessages, messages[i])
		}
	}

	return normalMessages, agentMessages
}

// processBatchMessages 处理一批同类型的消息
func (w *Webhook) processBatchMessages(
	messages []wkdb.Message,
	errMessageIDMap map[int64]int,
	messageType string,
	sendFunc func([]wkdb.Message) error,
) (bool, []wkdb.Message, error) {
	if len(messages) == 0 {
		return true, nil, nil
	}
	eligible := make([]wkdb.Message, 0, len(messages))
	paused := false
	for _, message := range messages {
		count := errMessageIDMap[message.MessageID]
		if count >= w.notifyRetryLimit() {
			paused = true
			delete(errMessageIDMap, message.MessageID)
			w.Warn("通知已持久保留并暂停自动补发，需要人工核验", zap.Int64("messageID", message.MessageID))
			continue
		}
		if count == 0 {
			exists, err := w.failures.Has(message.MessageID)
			if err != nil {
				w.Error("无法确认失败保留记录，暂停发送", zap.Error(err))
				return false, messages, err
			}
			if exists {
				// 相同 ID 若携带不同内容，必须报错保留缓存，不能把新证据静默丢弃。
				if err := w.failures.Save([]wkdb.Message{message}, "existing_failure"); err != nil {
					w.Error("已有失败通知内容冲突或无法确认，暂停发送", zap.Error(err))
					return false, messages, err
				}
				paused = true
				w.Warn("队列消息已有失败保留记录，不自动重放", zap.Int64("messageID", message.MessageID))
				continue
			}
		}
		eligible = append(eligible, message)
	}
	if len(eligible) == 0 {
		return !paused, nil, nil
	}
	if w.isStopping() {
		return false, eligible, nil
	}
	if err := sendFunc(eligible); err != nil {
		w.Error("Failed to send webhook for a batch of messages",
			zap.Error(err),
			zap.String("type", messageType),
			zap.Int("message_count", len(eligible)))
		remaining, archiveErr := w.handleSendFailure(eligible, errMessageIDMap)
		return false, remaining, archiveErr
	}
	// 远端已确认，清理证据失败只告警，不能把成功请求重新归为发送失败。
	confirmed := make([]int64, 0, len(eligible))
	for _, message := range eligible {
		if errMessageIDMap[message.MessageID] > 0 {
			confirmed = append(confirmed, message.MessageID)
		}
	}
	if len(confirmed) > 0 {
		if err := w.failures.Remove(confirmed); err != nil {
			w.Error("通知已确认但失败证据清理未完成，仅保留供人工核验", zap.Error(err), zap.Int("count", len(confirmed)))
		}
	}
	w.handleSendSuccess(eligible, errMessageIDMap, messageType)
	return !paused, nil, nil
}

// sendNormalMessages 发送普通消息
func (w *Webhook) sendNormalMessages(messages []wkdb.Message) error {
	messageResps := w.convertToMessageResps(messages)
	messageData, err := w.marshalMessages(messageResps)
	if err != nil {
		w.Error("Failed to marshal normal messages for webhook", zap.Error(err), zap.Int("message_count", len(messages)))
		return err
	}

	if options.G.WebhookGRPCOn() {
		return w.sendWebhookForGRPC(types.EventMsgNotify, messageData)
	}
	return w.sendWebhookForHttp(types.EventMsgNotify, messageData)
}

// sendAgentMessages 发送Agent消息
func (w *Webhook) sendAgentMessages(messages []wkdb.Message) error {
	messageResps := w.convertToMessageResps(messages)
	messageData, err := w.marshalMessages(messageResps)
	if err != nil {
		w.Error("Failed to marshal agent messages for webhook", zap.Error(err), zap.Int("message_count", len(messages)))
		return err
	}

	// TODO: Replace with the correct Agent Webhook URL from options.
	// The correct configuration option could not be determined automatically.
	return w.sendAgentWebhookForHttp(types.EventMsgNotify, messageData)
}

// convertToMessageResps 将wkdb.Message转换为MessageResp
func (w *Webhook) convertToMessageResps(messages []wkdb.Message) []*types.MessageResp {
	messageResps := make([]*types.MessageResp, 0, len(messages))
	for _, msg := range messages {
		resp := &types.MessageResp{}
		resp.From(msg, options.G.SystemUID)
		messageResps = append(messageResps, resp)
	}
	return messageResps
}

// handleSendFailure 处理发送失败的情况
func (w *Webhook) handleSendFailure(messages []wkdb.Message, errMessageIDMap map[int64]int) ([]wkdb.Message, error) {
	for _, msg := range messages {
		errMessageIDMap[msg.MessageID]++
	}
	if err := w.failures.Save(messages, "send_failed"); err != nil {
		w.Error("失败通知未能持久保留，暂停后续发送", zap.Error(err), zap.Int("count", len(messages)))
		return messages, err
	}
	retryableMessages := make([]wkdb.Message, 0, len(messages))
	for _, msg := range messages {
		if errMessageIDMap[msg.MessageID] >= w.notifyRetryLimit() {
			w.Warn("通知达到重试上限，已持久保留，等待人工核验", zap.Int64("messageID", msg.MessageID))
			delete(errMessageIDMap, msg.MessageID)
		} else {
			retryableMessages = append(retryableMessages, msg)
		}
	}
	return retryableMessages, nil
}

func (w *Webhook) notifyRetryLimit() int {
	if options.G.Webhook.MsgNotifyEventRetryMaxCount < 1 {
		return 1
	}
	return options.G.Webhook.MsgNotifyEventRetryMaxCount
}

func (w *Webhook) isStopping() bool {
	select {
	case <-w.stoped:
		return true
	default:
		return false
	}
}

func (w *Webhook) waitOrStop(duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-w.stoped:
		return false
	}
}

func (w *Webhook) requestContext() context.Context {
	if w.requestCtx != nil {
		return w.requestCtx
	}
	return context.Background()
}

// handleSendSuccess 处理发送成功的情况
func (w *Webhook) handleSendSuccess(messages []wkdb.Message, errMessageIDMap map[int64]int, messageType string) {
	for _, msg := range messages {
		delete(errMessageIDMap, msg.MessageID)
	}
	w.Info("Successfully pushed messages to webhook",
		zap.String("type", messageType),
		zap.Int("count", len(messages)))
}

// marshalMessages 序列化消息（可以后续优化为使用对象池）
func (w *Webhook) marshalMessages(messages []*types.MessageResp) ([]byte, error) {
	return json.Marshal(messages)
}

func (w *Webhook) loopOnlineStatus() {
	if !options.G.WebhookOn(types.EventOnlineStatus) {
		return
	}
	opLen := 0    // 最后一次操作在线状态数组的长度
	errCount := 0 // webhook请求失败重试次数
	for {
		if w.isStopping() {
			return
		}
		if opLen == 0 {
			w.onlinestatusLock.Lock()
			opLen = len(w.onlinestatusList)
			w.onlinestatusLock.Unlock()
		}
		if opLen == 0 {
			if !w.waitOrStop(time.Second * 2) {
				return
			}
			continue
		}
		w.onlinestatusLock.Lock()
		data := w.onlinestatusList[:opLen]
		w.onlinestatusLock.Unlock()
		jsonData, err := json.Marshal(data)
		if err != nil {
			w.Error("webhook的event数据不能json化！", zap.Error(err))
			if !w.waitOrStop(time.Second) {
				return
			}
			continue
		}

		if options.G.WebhookGRPCOn() {
			err = w.sendWebhookForGRPC(types.EventOnlineStatus, jsonData)
		} else {
			err = w.sendWebhookForHttp(types.EventOnlineStatus, jsonData)
		}
		if err != nil {
			errCount++
			w.Error("请求在线状态webhook失败！", zap.Error(err))
			if errCount >= options.G.Webhook.MsgNotifyEventRetryMaxCount {
				w.Error("请求在线状态webhook失败通知超过最大次数！", zap.Int("MsgNotifyEventRetryMaxCount", options.G.Webhook.MsgNotifyEventRetryMaxCount))

				w.onlinestatusLock.Lock()
				w.onlinestatusList = w.onlinestatusList[opLen:]
				opLen = 0
				w.onlinestatusLock.Unlock()

				errCount = 0
			}

			if !w.waitOrStop(time.Second) {
				return
			}
			continue
		}

		w.onlinestatusLock.Lock()
		w.onlinestatusList = w.onlinestatusList[opLen:]
		opLen = 0
		w.onlinestatusLock.Unlock()

	}
}

func (w *Webhook) sendWebhookForHttp(event string, data []byte) error {
	eventURL := fmt.Sprintf("%s?event=%s", options.G.Webhook.HTTPAddr, event)
	startTime := time.Now().UnixNano() / 1000 / 1000
	w.Debug("webhook开始请求", zap.String("eventURL", eventURL))
	req, err := http.NewRequestWithContext(w.requestContext(), http.MethodPost, eventURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.httpClient.Do(req)
	w.Debug("webhook请求结束 耗时", zap.Int64("mill", time.Now().UnixNano()/1000/1000-startTime))
	if err != nil {
		w.Warn("调用第三方消息通知失败！", zap.String("Webhook", options.G.Webhook.HTTPAddr), zap.Error(err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		w.Warn("第三方消息通知接口返回状态错误！", zap.Int("status", resp.StatusCode), zap.String("Webhook", options.G.Webhook.HTTPAddr))
		return errors.New("第三方消息通知接口返回状态错误！")
	}
	return nil
}

func (w *Webhook) sendAgentWebhookForHttp(event string, data []byte) error {
	// TODO: Replace options.G.Webhook.HTTPAddr with the correct Agent Webhook URL from options.
	// The correct configuration option could not be determined automatically.
	agentWebhookAddr := options.G.Agent.Webhook.HTTPAddr
	eventURL := fmt.Sprintf("%s?event=%s", agentWebhookAddr, event)
	startTime := time.Now().UnixNano() / 1000 / 1000
	w.Debug("agent webhook开始请求", zap.String("eventURL", eventURL))
	req, err := http.NewRequestWithContext(w.requestContext(), http.MethodPost, eventURL, bytes.NewBuffer(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.httpClient.Do(req)
	w.Debug("agent webhook请求结束 耗时", zap.Int64("mill", time.Now().UnixNano()/1000/1000-startTime))
	if err != nil {
		w.Warn("调用第三方agent消息通知失败！", zap.String("Webhook", agentWebhookAddr), zap.Error(err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		w.Warn("第三方agent消息通知接口返回状态错误！", zap.Int("status", resp.StatusCode), zap.String("Webhook", agentWebhookAddr))
		return errors.New("第三方agent消息通知接口返回状态错误！")
	}
	return nil
}

func (w *Webhook) sendWebhookForGRPC(event string, data []byte) error {

	startNow := time.Now()
	startTime := startNow.UnixNano() / 1000 / 1000
	w.Debug("webhook grpc 开始请求", zap.String("event", event))

	ctx, cancel := context.WithTimeout(w.requestContext(), time.Second*2)
	defer cancel()
	clientConn, err := w.webhookGRPCPool.Get(ctx)
	if err != nil {
		return err
	}
	defer clientConn.Close()
	// cliConn, err := grpc.Dial(l.opts.WebhookGRPC, grpc.WithInsecure())
	// if err != nil {
	// 	return err
	// }
	cli := wkhook.NewWebhookServiceClient(clientConn)

	sendCtx, sendCancel := context.WithTimeout(w.requestContext(), time.Second*10)
	defer sendCancel()
	resp, err := cli.SendWebhook(sendCtx, &wkhook.EventReq{
		Event: event,
		Data:  data,
	})
	w.Debug("webhook grpc 请求结束 耗时", zap.Int64("mill", time.Now().UnixNano()/1000/1000-startTime))

	if err != nil {
		return err
	}
	if resp.Status != wkhook.EventStatus_Success {
		return errors.New("grpc返回状态错误！")
	}
	return nil
}

var (
	// eventWebHook 用于快速校验用用户配置的关注事件
	eventWebHook = map[string]map[string]struct{}{
		types.EventMsgOffline:   {},
		types.EventMsgNotify:    {},
		types.EventOnlineStatus: {},
	}
)
