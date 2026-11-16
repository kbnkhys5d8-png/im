package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	nodetypes "github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func legacyTestChannel(channelID string, channelType uint8, seqs ...uint64) *channelRecentMessage {
	channel := &channelRecentMessage{ChannelId: channelID, ChannelType: channelType}
	for _, seq := range seqs {
		channel.Messages = append(channel.Messages, &types.MessageResp{MessageSeq: seq})
	}
	return channel
}

func TestLegacySyncPageRecordsOnlyReturnedChannelMessages(t *testing.T) {
	channels := []*channelRecentMessage{
		{ChannelId: "a____cmd", ChannelType: 1, Messages: []*types.MessageResp{{MessageSeq: 100}, {MessageSeq: 101}}},
		{ChannelId: "b____cmd", ChannelType: 2, Messages: []*types.MessageResp{{MessageSeq: 7}, {MessageSeq: 8}}},
	}
	tests := []struct {
		name    string
		limit   int
		seqs    []uint64
		records []syncRecord
	}{
		{"首页填满后不记录后续频道", 2, []uint64{100, 101}, []syncRecord{{"a____cmd", 1, 101}}},
		{"跨频道截断时仅记录本频道实际返回的最后一条", 3, []uint64{100, 101, 7}, []syncRecord{{"a____cmd", 1, 101}, {"b____cmd", 2, 7}}},
		{"旧回执字段对应响应的最后一条而不是跨频道最高序号", 4, []uint64{100, 101, 7, 8}, []syncRecord{{"a____cmd", 1, 101}, {"b____cmd", 2, 8}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 模拟多个分片的返回顺序变化，重试仍应得到同一页及相同的回执边界。
			reversed := []*channelRecentMessage{channels[1], channels[0]}
			messages, records := legacySyncPage(legacySyncSortedChannels(reversed), tc.limit, legacySyncCutoff{})
			var seqs []uint64
			for _, message := range messages {
				seqs = append(seqs, message.MessageSeq)
			}
			if !slices.Equal(seqs, tc.seqs) || !slices.Equal(records, tc.records) {
				t.Fatalf("响应 = %+v, 回执水位 = %+v; 要响应 %+v, 水位 %+v", seqs, records, tc.seqs, tc.records)
			}
		})
	}
}

func TestLegacySyncVisibleOffsetAppliesBeforeLimitAndAck(t *testing.T) {
	var batches legacySyncBatches
	now := time.Unix(1, 0)
	a := legacyTestChannel("a____cmd", 2, 1, 5, 6)
	b := legacyTestChannel("b____cmd", 2, 2, 7)
	channels := []*channelRecentMessage{a, b}
	page := batches.publish("uid", channels, 2, legacySyncCutoff{"a____cmd", 2, 5}, now)
	if !slices.Equal(page, []*types.MessageResp{a.Messages[2], b.Messages[0]}) {
		t.Fatalf("不可见前缀不能消耗页长度，且不能过滤其他频道：%+v", page)
	}
	batch, err := batches.beginAck("uid", 2, now)
	if err != nil || batch == nil || !slices.Equal(batch.records, []syncRecord{{"a____cmd", 2, 6}, {"b____cmd", 2, 2}}) {
		t.Fatalf("回执仅登记客户端所见消息：%v, %v", batch, err)
	}
}

func TestLegacySyncVisibilityUsesOnlyExplicitStorageKeyAndType(t *testing.T) {
	opts := options.New()
	for _, tc := range []struct {
		name, public, storage string
		channelType           uint8
	}{
		{"群聊", "target", "target", wkproto.ChannelTypeGroup},
		{"单聊", "peer", options.GetFakeChannelIDWith("viewer", "peer"), wkproto.ChannelTypePerson},
		{"Agent", "bot", options.GetAgentChannelIDWith("viewer", "bot"), wkproto.ChannelTypeAgent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			physical := opts.OrginalConvertCmdChannel(tc.storage)
			otherType := tc.channelType + 1
			channels := []wkdb.Conversation{
				{ChannelId: physical, ChannelType: tc.channelType, ReadToMsgSeq: 3},
				{ChannelId: physical, ChannelType: otherType, ReadToMsgSeq: 1},
				{ChannelId: opts.OrginalConvertCmdChannel("other"), ChannelType: tc.channelType},
			}
			ordinary := (&legacySyncVisibility{ChannelID: tc.public, ChannelType: tc.channelType, AfterSeq: 5}).forStorage(opts)
			queries := legacySyncQueries(channels, ordinary)
			if len(queries) != 3 || queries[0].LastMsgSeq != 4 {
				t.Fatalf("普通频道的序号不能应用到 CMD 日志：%+v", queries)
			}
			cmd := (&legacySyncVisibility{ChannelID: physical, ChannelType: tc.channelType, AfterSeq: 5}).forStorage(opts)
			queries = legacySyncQueries(channels, cmd)
			if len(queries) != 3 || queries[0].LastMsgSeq != 6 || queries[1].LastMsgSeq != 2 || queries[2].LastMsgSeq != 1 {
				t.Fatalf("只应跳过明确匹配的 CMD 频道历史前缀：%+v", queries)
			}
		})
	}
}

func TestLegacySyncScopedHTTPResponsePreservesOtherSequenceDomains(t *testing.T) {
	previousOptions, previousStore, previousCluster, previousManager := options.G, service.Store, service.Cluster, service.ConversationManager
	options.G = options.New()
	options.G.Cluster.NodeId = 1
	service.Store = store.New(store.NewOptions(store.WithDB(legacySyncHTTPDB{})))
	service.Cluster = legacySyncHTTPCluster{}
	service.ConversationManager = legacySyncHTTPConversationManager{}
	t.Cleanup(func() {
		options.G, service.Store, service.Cluster, service.ConversationManager = previousOptions, previousStore, previousCluster, previousManager
	})
	for _, tc := range []struct {
		name, body       string
		scoped, disabled bool
		seqs             []uint64
	}{
		{"旧调用仍返回数组", `{"uid":"user","limit":3}`, false, false, []uint64{1, 5, 6}},
		{"普通频道不可裁剪同名或其他 CMD", `{"uid":"user","limit":4,"visibility":{"channel_id":"a","channel_type":2,"after_seq":5}}`, true, false, []uint64{1, 5, 6, 2}},
		{"明确物理键只裁剪该 CMD", `{"uid":"user","limit":2,"visibility":{"channel_id":"a____cmd","channel_type":2,"after_seq":5}}`, true, false, []uint64{6, 2}},
		{"同步关闭时旧数组", `{"uid":"user"}`, false, true, []uint64{}},
		{"同步关闭时新对象", `{"uid":"user","visibility":{"channel_id":"a","channel_type":2,"after_seq":5}}`, true, true, []uint64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options.G.DisableCMDMessageSync = tc.disabled
			server := &Server{}
			server.requset = newRequset(server)
			router := wkhttp.New()
			router.POST("/message/sync", newMessage(server).sync)
			response := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/message/sync", strings.NewReader(tc.body))
			router.GetGinRoute().ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("状态码 %d，响应 %s", response.Code, response.Body)
			}
			var messages []*types.MessageResp
			if tc.scoped {
				var envelope struct {
					Messages []*types.MessageResp `json:"messages"`
				}
				if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
					t.Fatalf("期望内部对象：%v, %s", err, response.Body)
				}
				messages = envelope.Messages
			} else if err := json.Unmarshal(response.Body.Bytes(), &messages); err != nil {
				t.Fatalf("期望旧数组：%v, %s", err, response.Body)
			}
			seqs := make([]uint64, 0, len(messages))
			for _, m := range messages {
				seqs = append(seqs, m.MessageSeq)
			}
			if messages == nil || !slices.Equal(seqs, tc.seqs) {
				t.Fatalf("序号 = %v，要 %v；响应：%s", seqs, tc.seqs, response.Body)
			}
		})
	}
}

type legacySyncHTTPDB struct{ wkdb.DB }

func (legacySyncHTTPDB) GetConversationsByType(string, wkdb.ConversationType) ([]wkdb.Conversation, error) {
	return []wkdb.Conversation{{ChannelId: "a____cmd", ChannelType: 2}, {ChannelId: "b____cmd", ChannelType: 2}}, nil
}

func (legacySyncHTTPDB) GetChannelShardIndex(string, uint8) uint32 { return 0 }

func (legacySyncHTTPDB) GetUserLastMsgSeqBatch(string, []wkdb.Channel) (map[string]uint64, error) {
	return nil, nil
}

func (legacySyncHTTPDB) LoadMsgsBatch(requests []wkdb.BatchMsgRequest) ([]wkdb.BatchMsgResponse, error) {
	var responses []wkdb.BatchMsgResponse
	for _, req := range requests {
		seqs := []uint32{2, 7}
		if req.ChannelId == "a____cmd" {
			seqs = []uint32{1, 5, 6}
		}
		resp := wkdb.BatchMsgResponse{ChannelId: req.ChannelId, ChannelType: req.ChannelType}
		for _, seq := range seqs {
			if uint64(seq) >= req.MsgSeq && len(resp.Messages) < req.Limit {
				// 响应中的业务频道 ID 不用于判断存储范围。
				resp.Messages = append(resp.Messages, wkdb.Message{RecvPacket: wkproto.RecvPacket{
					ChannelID: "logical-channel", ChannelType: req.ChannelType, MessageSeq: seq,
				}})
			}
		}
		responses = append(responses, resp)
	}
	return responses, nil
}

type legacySyncHTTPCluster struct{ icluster.ICluster }

func (legacySyncHTTPCluster) SlotLeaderOfChannel(string, uint8) (*nodetypes.Node, error) {
	return &nodetypes.Node{Id: 1}, nil
}

func (legacySyncHTTPCluster) LeaderOfChannel(string, uint8) (*nodetypes.Node, error) {
	return &nodetypes.Node{Id: 1}, nil
}

type legacySyncHTTPConversationManager struct{ service.IConversationManager }

func (legacySyncHTTPConversationManager) GetUserChannelsFromCache(string, wkdb.ConversationType) ([]wkproto.Channel, error) {
	return nil, nil
}

func TestLegacySyncStartUsesLargerBoundWithoutOverflow(t *testing.T) {
	for _, tc := range []struct {
		readTo, visibleAfter, want uint64
		ok                         bool
	}{
		{0, 0, 1, true},
		{2, 10, 11, true},
		{20, 10, 21, true},
		{wkdb.MaxMessageSequence - 1, 0, wkdb.MaxMessageSequence, true},
		{wkdb.MaxMessageSequence, 1, 0, false},
		{2, ^uint64(0), 0, false},
	} {
		if start, ok := legacySyncStart(tc.readTo, tc.visibleAfter); start != tc.want || ok != tc.ok {
			t.Errorf("起点 (%d, %d) = (%d, %t)，期望 (%d, %t)", tc.readTo, tc.visibleAfter, start, ok, tc.want, tc.ok)
		}
	}
}

func TestLegacySyncBatchesStaleAckDoesNotConsumeNewerBatch(t *testing.T) {
	now := time.Unix(1, 0)
	var batches legacySyncBatches
	old := []syncRecord{{"a____cmd", 1, 100}}
	newer := []syncRecord{{"b____cmd", 2, 200}}
	// 后发的请求可能先返回，按回执序号隔离使晚到的旧请求不能覆盖它。
	batches.publish("uid", []*channelRecentMessage{legacyTestChannel("b____cmd", 2, 200)}, 1, legacySyncCutoff{}, now)
	batches.publish("uid", []*channelRecentMessage{legacyTestChannel("a____cmd", 1, 100)}, 1, legacySyncCutoff{}, now)
	if batch, err := batches.beginAck("uid", 99, now); err != nil || batch != nil {
		t.Fatalf("未知旧回执不应消费任何批次：%v, %v", batch, err)
	}
	oldBatch, err := batches.beginAck("uid", 100, now)
	if err != nil || oldBatch == nil || !slices.Equal(oldBatch.records, old) {
		t.Fatalf("旧回执只能消费自己的批次：%v, %v", oldBatch, err)
	}
	if batch, err := batches.beginAck("uid", 200, now); err == nil || batch != nil {
		t.Fatalf("同一用户的游标提交应串行：%v, %v", batch, err)
	}
	batches.finishAck("uid", oldBatch, true, now)
	if batch, err := batches.beginAck("uid", 100, now); err != nil || batch != nil {
		t.Fatalf("重复回执不应再次消费：%v, %v", batch, err)
	}
	newBatch, err := batches.beginAck("uid", 200, now)
	if err != nil || newBatch == nil || !slices.Equal(newBatch.records, newer) {
		t.Fatalf("新批次必须保持可确认：%v, %v", newBatch, err)
	}
}

func TestLegacySyncBatchesReturnSequentialEqualSequencePagesWithoutMisapplyingAck(t *testing.T) {
	now := time.Unix(1, 0)
	var batches legacySyncBatches
	a, b := legacyTestChannel("a____cmd", 1, 8), legacyTestChannel("b____cmd", 2, 8)
	for i := 0; i < 2; i++ {
		if page := batches.publish("uid", []*channelRecentMessage{a, b}, 1, legacySyncCutoff{}, now); !slices.Equal(page, a.Messages) {
			t.Fatalf("未确认的同一页应能重试：%v", page)
		}
	}
	batch, err := batches.beginAck("uid", 8, now)
	if err != nil || batch == nil || !slices.Equal(batch.records, []syncRecord{{"a____cmd", 1, 8}}) {
		t.Fatalf("旧回执应确认首个页面：%v, %v", batch, err)
	}
	batches.finishAck("uid", batch, true, now)
	for _, retry := range []time.Duration{time.Second, time.Minute, 4 * time.Minute} {
		at := now.Add(retry)
		if page := batches.publish("uid", []*channelRecentMessage{b}, 1, legacySyncCutoff{}, at); !slices.Equal(page, b.Messages) {
			t.Fatalf("第一批确认后同序号的新页也应正常返回：%v", page)
		}
		if batch, err := batches.beginAck("uid", 8, at); err != nil || batch != nil {
			t.Fatalf("可能是第一批迟到的确认，不能据此推进新页：%v, %v", batch, err)
		}
	}
	after := now.Add(legacySyncBatchRetention + time.Second)
	batches.publish("uid", []*channelRecentMessage{b}, 1, legacySyncCutoff{}, after)
	if batch, err := batches.beginAck("uid", 8, after); err != nil || batch == nil || !slices.Equal(batch.records, []syncRecord{{"b____cmd", 2, 8}}) {
		t.Fatalf("重复补拉不应延长冲突保护期：%v, %v", batch, err)
	}
}

func TestLegacySyncBatchesConcurrentPagesWithEqualSequenceDoNotAdvance(t *testing.T) {
	now := time.Unix(1, 0)
	var batches legacySyncBatches
	start := make(chan struct{})
	a, b := legacyTestChannel("a____cmd", 1, 8), legacyTestChannel("b____cmd", 2, 8)
	results := make(chan []*types.MessageResp, 2)
	for _, channel := range []*channelRecentMessage{a, b} {
		go func() {
			<-start
			results <- batches.publish("uid", []*channelRecentMessage{channel}, 1, legacySyncCutoff{}, now)
		}()
	}
	close(start)
	first, second := <-results, <-results
	if len(first) != 1 || len(second) != 1 || first[0] == second[0] {
		t.Fatalf("两个并发页面都应返回：%v, %v", first, second)
	}
	batch, err := batches.beginAck("uid", 8, now)
	if err != nil || batch != nil {
		t.Fatalf("回执不能推进任一冲突页面：%v, %v", batch, err)
	}
}

func TestLegacySyncBatchesLateAckFromRecentlyReturnedConflictIsNotRebound(t *testing.T) {
	now := time.Unix(1, 0)
	a, b, c := legacyTestChannel("a____cmd", 1, 8), legacyTestChannel("b____cmd", 2, 8), legacyTestChannel("c____cmd", 2, 8)
	for _, tc := range []struct {
		name string
		near *channelRecentMessage
	}{
		{"保护期将满时返回第三种页面", c},
		{"保护期将满时重复返回第二种页面", b},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var batches legacySyncBatches
			batches.publish("uid", []*channelRecentMessage{a}, 1, legacySyncCutoff{}, now)
			batches.publish("uid", []*channelRecentMessage{b}, 1, legacySyncCutoff{}, now)
			near := now.Add(legacySyncBatchRetention - time.Millisecond)
			batches.publish("uid", []*channelRecentMessage{tc.near}, 1, legacySyncCutoff{}, near)
			after := now.Add(legacySyncBatchRetention + time.Millisecond)
			batches.publish("uid", []*channelRecentMessage{a}, 1, legacySyncCutoff{}, after)
			if batch, err := batches.beginAck("uid", 8, after); err != nil || batch != nil {
				t.Fatalf("旧保护期结束不代表刚返回的冲突页已失效：%v, %v", batch, err)
			}
			ready := after.Add(legacySyncBatchRetention)
			batches.publish("uid", []*channelRecentMessage{a}, 1, legacySyncCutoff{}, ready)
			if batch, err := batches.beginAck("uid", 8, ready); err != nil || batch == nil || !slices.Equal(batch.records, []syncRecord{{"a____cmd", 1, 8}}) {
				t.Fatalf("稳定重发同一页后应恢复确认：%v, %v", batch, err)
			}
		})
	}
}

func TestLegacySyncBatchesCollisionDoesNotBlockIndependentPages(t *testing.T) {
	var batches legacySyncBatches
	now := time.Unix(1, 0)
	old := legacyTestChannel("old____cmd", 1, 8)
	batches.publish("uid", []*channelRecentMessage{old}, 1, legacySyncCutoff{}, now)
	conflict, good := legacyTestChannel("a____cmd", 1, 8), legacyTestChannel("b____cmd", 2, 9)
	if page := batches.publish("uid", []*channelRecentMessage{conflict, good}, 1, legacySyncCutoff{}, now); !slices.Equal(page, good.Messages) {
		t.Fatalf("冲突频道不应挡住后续可确认频道：%v", page)
	}
	batch, err := batches.beginAck("uid", 9, now)
	if err != nil || batch == nil || !slices.Equal(batch.records, []syncRecord{{"b____cmd", 2, 9}}) {
		t.Fatalf("替代页面只应推进已返回频道：%v, %v", batch, err)
	}
	batches.finishAck("uid", batch, true, now)
	goodPrefix := legacyTestChannel("a____cmd", 1, 6, 8)
	if page := batches.publish("uid", []*channelRecentMessage{goodPrefix}, 2, legacySyncCutoff{}, now); !slices.Equal(page, goodPrefix.Messages[:1]) {
		t.Fatalf("尾序号冲突时应保留可确认前缀：%v", page)
	}
	if batch, err := batches.beginAck("uid", 6, now); err != nil || batch == nil || !slices.Equal(batch.records, []syncRecord{{"a____cmd", 1, 6}}) {
		t.Fatalf("替代前缀不能推进到未发送的尾序号：%v, %v", batch, err)
	}
}

func TestLegacySyncBatchesFullDoesNotMisapplyLateAckFromRecentOverflow(t *testing.T) {
	now := time.Unix(1, 0)
	var batches legacySyncBatches
	for seq := uint64(1); seq <= legacySyncMaxBatchesPerUID; seq++ {
		batches.publish("uid", []*channelRecentMessage{legacyTestChannel("a____cmd", 1, seq)}, 1, legacySyncCutoff{}, now)
	}
	seq := uint64(legacySyncMaxBatchesPerUID + 1)
	a, b := legacyTestChannel("a____cmd", 1, seq), legacyTestChannel("b____cmd", 2, seq)
	batches.publish("uid", []*channelRecentMessage{a}, 1, legacySyncCutoff{}, now)
	near := now.Add(legacySyncBatchRetention - time.Millisecond)
	batches.publish("uid", []*channelRecentMessage{b}, 1, legacySyncCutoff{}, near)
	after := now.Add(legacySyncBatchRetention + time.Millisecond)
	batches.publish("uid", []*channelRecentMessage{a}, 1, legacySyncCutoff{}, after)
	if batch, err := batches.beginAck("uid", seq, after); err != nil || batch != nil {
		t.Fatalf("释放过期容量后也不能误认刚发出的未登记页回执：%v, %v", batch, err)
	}
	ready := after.Add(legacySyncBatchRetention)
	batches.publish("uid", []*channelRecentMessage{a}, 1, legacySyncCutoff{}, ready)
	if batch, err := batches.beginAck("uid", seq, ready); err != nil || batch == nil || !slices.Equal(batch.records, []syncRecord{{"a____cmd", 1, seq}}) {
		t.Fatalf("容量和保护期恢复后应能确认：%v, %v", batch, err)
	}
}

func TestLegacySyncBatchesAckCanRetryAfterStoreError(t *testing.T) {
	now := time.Unix(1, 0)
	var batches legacySyncBatches
	batches.publish("uid", []*channelRecentMessage{legacyTestChannel("a____cmd", 1, 8)}, 1, legacySyncCutoff{}, now)
	batch, err := batches.beginAck("uid", 8, now)
	if err != nil || batch == nil {
		t.Fatalf("批次应可确认：%v, %v", batch, err)
	}
	batches.finishAck("uid", batch, false, now)
	retry, err := batches.beginAck("uid", 8, now.Add(time.Second))
	if err != nil || retry != batch {
		t.Fatalf("写入失败后回执应可重试：%v, %v", retry, err)
	}
}

func TestLegacySyncBatchesKeepsBoundedProtectionWithoutEviction(t *testing.T) {
	now := time.Unix(1, 0)
	var batches legacySyncBatches
	for seq := uint64(1); seq <= legacySyncMaxBatchesPerUID; seq++ {
		batches.publish("uid", []*channelRecentMessage{legacyTestChannel("a____cmd", 1, seq)}, 1, legacySyncCutoff{}, now)
	}
	nextSeq := uint64(legacySyncMaxBatchesPerUID + 1)
	newPage := legacyTestChannel("a____cmd", 1, nextSeq)
	if page := batches.publish("uid", []*channelRecentMessage{newPage}, 1, legacySyncCutoff{}, now.Add(time.Second)); !slices.Equal(page, newPage.Messages) {
		t.Fatalf("已满时仍应返回页面：%v", page)
	}
	if batch, err := batches.beginAck("uid", nextSeq, now.Add(time.Second)); err != nil || batch != nil {
		t.Fatalf("不能确认未登记的页面：%v, %v", batch, err)
	}
	earliest, err := batches.beginAck("uid", 1, now.Add(time.Second))
	if err != nil || earliest == nil {
		t.Fatalf("旧批次仍应可确认：%v, %v", earliest, err)
	}
	batches.finishAck("uid", earliest, true, now.Add(time.Second))
	protected := now.Add(legacySyncBatchRetention)
	batches.publish("uid", []*channelRecentMessage{newPage}, 1, legacySyncCutoff{}, protected)
	if batch, err := batches.beginAck("uid", nextSeq, protected); err != nil || batch != nil {
		t.Fatalf("不能过早绑定未登记页面的旧回执：%v, %v", batch, err)
	}
	after := protected.Add(time.Second)
	batches.publish("uid", []*channelRecentMessage{newPage}, 1, legacySyncCutoff{}, after)
	if batch, err := batches.beginAck("uid", nextSeq, after); err != nil || batch == nil {
		t.Fatalf("保护期结束后应能确认新页：%v, %v", batch, err)
	}
}
