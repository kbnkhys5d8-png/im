package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type historyAPISlot struct {
	icluster.Slot
	store *store.Store
}

func (s *historyAPISlot) GetSlotId(string) uint32 { return 0 }
func (s *historyAPISlot) ProposeUntilAppliedTimeout(_ context.Context, _ uint32, data []byte) (*rafttypes.ProposeResp, error) {
	return &rafttypes.ProposeResp{}, s.store.ApplySlotLogs(0, []rafttypes.Log{{Index: 1, Data: data}})
}

func historyAPIFixture(t *testing.T, sizes ...int) (*wkhttp.WKHttp, wkdb.DB, historyRequest) {
	t.Helper()
	previous := options.G
	options.G = options.New()
	options.G.ManagerToken = "synthetic-history-token"
	t.Cleanup(func() { options.G = previous })
	db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(t.TempDir()), wkdb.WithShardNum(1)))
	if err := db.Open(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	slot := &historyAPISlot{}
	s := store.New(store.NewOptions(store.WithDB(db), store.WithSlot(slot), store.WithHistoryTopology(func(string, uint8) error { return nil }), store.WithHistoryCapabilities(func() error { return nil }), store.WithHistoryDrain(func(context.Context, string, uint8) error { return nil })))
	slot.store = s
	r := historyRequest{TaskID: strings.Repeat("a", 64), SourceUID: "a", PeerUID: "c", Limit: 200}
	identity, err := r.identity()
	if err != nil {
		t.Fatal(err)
	}
	for i, size := range sizes {
		msg := wkdb.Message{RecvPacket: wkproto.RecvPacket{MessageID: int64(i + 100), MessageSeq: uint32(i + 1), FromUID: "a", ChannelID: identity.ChannelID, ChannelType: 1, Timestamp: 1700000000, Payload: []byte(strings.Repeat("x", size))}}
		if err := db.AppendMessages(identity.ChannelID, 1, []wkdb.Message{msg}); err != nil {
			t.Fatal(err)
		}
	}
	router := wkhttp.New()
	(&historyAPI{store: s}).route(router)
	return router, db, r
}
func historyHTTP(router *wkhttp.WKHttp, path string, body any, remote, token string) *httptest.ResponseRecorder {
	data, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
	req.RemoteAddr = remote
	req.Header.Set("token", token)
	req.Header.Set("X-Forwarded-For", "127.0.0.1")
	response := httptest.NewRecorder()
	router.GetGinRoute().ServeHTTP(response, req)
	return response
}
func historyLocalHTTP(router *wkhttp.WKHttp, path string, body any) *httptest.ResponseRecorder {
	return historyHTTP(router, path, body, "127.0.0.1:30000", "synthetic-history-token")
}

func TestHistoryHTTPIdentityAndLoopback(t *testing.T) {
	router, _, request := historyAPIFixture(t, 20)
	for _, path := range []string{"/history/capabilities", "/history/fence", "/history/export", "/history/fence/release"} {
		for _, remote := range []string{"203.0.113.1:1234", "localhost:1234", "invalid"} {
			if r := historyHTTP(router, path, request, remote, "synthetic-history-token"); r.Code != 403 {
				t.Fatalf("代理头绕过 %s: %d", path, r.Code)
			}
		}
		if r := historyHTTP(router, path, request, "[::1]:30000", "bad"); r.Code != 403 {
			t.Fatal(r.Code)
		}
	}
	for _, mutate := range []func(*historyRequest){
		func(r *historyRequest) { r.TaskID = strings.Repeat("A", 64) }, func(r *historyRequest) { r.TaskID = "short" },
		func(r *historyRequest) { r.SourceUID = "a@x" }, func(r *historyRequest) { r.PeerUID = r.SourceUID },
		func(r *historyRequest) { r.SourceUID = " a" },
		func(r *historyRequest) { r.SourceUID = "plumless"; r.PeerUID = "buckeroo" },
	} {
		r := request
		mutate(&r)
		if result := historyLocalHTTP(router, "/history/fence", r); result.Code == 200 {
			t.Fatalf("身份获准: %+v", r)
		}
	}
	cap := historyLocalHTTP(router, "/history/capabilities", map[string]any{})
	if cap.Code != 200 || !strings.Contains(cap.Body.String(), `"protocol":1`) {
		t.Fatal(cap.Body.String())
	}
	result := historyHTTP(router, "/history/fence", request, "[::1]:1234", "synthetic-history-token")
	if result.Code != 200 {
		t.Fatal(result.Body.String())
	}
	var f wkdb.HistoryFence
	if err := json.Unmarshal(result.Body.Bytes(), &f); err != nil {
		t.Fatal(err)
	}
	if f.ChannelID != options.GetFakeChannelIDWith("a", "c") || f.HighWatermark != 1 || !strings.Contains(result.Body.String(), `"source_channel"`) {
		t.Fatal(result.Body.String())
	}
	other := request
	other.TaskID = strings.Repeat("b", 64)
	for _, path := range []string{"/history/fence", "/history/export", "/history/fence/release"} {
		if result := historyLocalHTTP(router, path, other); result.Code == 200 {
			t.Fatal("任务冲突被接受", path)
		}
	}
	for i := 0; i < 2; i++ {
		if result := historyLocalHTTP(router, "/history/fence/release", request); result.Code != 200 {
			t.Fatal(result.Body.String())
		}
	}
}

func TestHistoryHTTPExportBudgetNoLossAndTerminalCursor(t *testing.T) {
	// 原始正文不足 4MB，原生 JSON base64 后超限，必须覆盖 DTO 截页而不是只测底层截页。
	router, db, request := historyAPIFixture(t, 1800*1024, 1800*1024)
	if r := historyLocalHTTP(router, "/history/fence", request); r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	seen := map[int64]bool{}
	for step := 0; step < 3; step++ {
		r := historyLocalHTTP(router, "/history/export", request)
		if r.Code != 200 {
			t.Fatal(r.Body.String())
		}
		if r.Body.Len() > store.HistoryPageBytes {
			t.Fatal("JSON 超过 4MB")
		}
		var page historyResponse
		if err := json.Unmarshal(r.Body.Bytes(), &page); err != nil {
			t.Fatal(err)
		}
		if page.SourceChannel != options.GetFakeChannelIDWith("a", "c") || page.HighWatermark != 2 {
			t.Fatal(page)
		}
		for _, m := range page.Messages {
			if m.ChannelID != page.SourceChannel {
				t.Fatal("导出把 canonical 频道改成了 logical UID")
			}
			if seen[m.MessageId] {
				t.Fatal("重复ID")
			}
			seen[m.MessageId] = true
		}
		if page.More && page.NextSeq <= request.AfterSeq {
			t.Fatal("游标未前进")
		}
		request.AfterSeq = page.NextSeq
		if !page.More {
			if page.NextSeq != 2 {
				t.Fatal(page)
			}
			break
		}
	}
	if len(seen) != 2 {
		t.Fatal(seen)
	}
	r := historyLocalHTTP(router, "/history/export", request)
	var tail historyResponse
	if err := json.Unmarshal(r.Body.Bytes(), &tail); err != nil {
		t.Fatal(err)
	}
	if tail.NextSeq != 2 || tail.More || len(tail.Messages) != 0 {
		t.Fatal(tail)
	}
	seq, _, err := db.GetChannelLastMessageSeq(options.GetFakeChannelIDWith("a", "c"), 1)
	if err != nil || seq != 2 {
		t.Fatal(seq, err)
	}
	for _, limit := range []int{0, 201} {
		bad := request
		bad.Limit = limit
		if r := historyLocalHTTP(router, "/history/export", bad); r.Code == 200 {
			t.Fatal("非法页大小")
		}
	}
}

func TestHistoryHTTPOversizeMessageFailsWithoutCursor(t *testing.T) {
	router, _, request := historyAPIFixture(t, store.HistoryPageBytes)
	if r := historyLocalHTTP(router, "/history/fence", request); r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	if r := historyLocalHTTP(router, "/history/export", request); r.Code == 200 {
		t.Fatal("单条超限被静默跳过")
	}
}

func TestHistoryHTTPCapabilitiesRejectsNotReady(t *testing.T) {
	previous := options.G
	options.G = options.New()
	options.G.ManagerToken = "synthetic-history-token"
	defer func() { options.G = previous }()
	router := wkhttp.New()
	(&historyAPI{store: store.New(store.NewOptions())}).route(router)
	if r := historyLocalHTTP(router, "/history/capabilities", map[string]any{}); r.Code != 501 {
		t.Fatal(r.Code)
	}
}

func TestHistoryHTTPPreservesNativeIdentityAndRejectsStreams(t *testing.T) {
	for _, mode := range []string{"plain", "system_sender", "stream_no", "large_stream_no", "stream_setting", "empty_tail"} {
		t.Run(mode, func(t *testing.T) {
			router, db, request := historyAPIFixture(t)
			identity, _ := request.identity()
			var setting wkproto.Setting
			setting = setting.Set(wkproto.SettingSignal)
			// 未解释的设置位也必须原样导出，不能只重建已知标记。
			setting = wkproto.Setting(setting.Uint8() | 0x80)
			msg := wkdb.Message{RecvPacket: wkproto.RecvPacket{
				Framer:    wkproto.Framer{NoPersist: true, RedDot: true, SyncOnce: true},
				MessageID: 981, MessageSeq: 1, FromUID: "a", ChannelID: identity.ChannelID, ChannelType: 1,
				Setting: setting, Expire: 99, Timestamp: 1700000001,
				Payload: []byte(`{"type":1,"content":"逐字段合成"}`),
			}}
			if mode == "system_sender" {
				msg.FromUID = options.G.SystemUID
			}
			if mode == "stream_no" || mode == "large_stream_no" {
				msg.StreamNo = "unsupported-stream"
			}
			if mode == "large_stream_no" {
				msg.Payload = bytes.Repeat([]byte("x"), store.HistoryPageBytes)
			}
			if mode == "stream_setting" {
				msg.Setting = msg.Setting.Set(wkproto.SettingStream)
			}
			if err := db.AppendMessages(identity.ChannelID, 1, []wkdb.Message{msg}); err != nil {
				t.Fatal(err)
			}
			if mode == "empty_tail" {
				if err := db.SetChannelLastMessageSeq(identity.ChannelID, 1, 3); err != nil {
					t.Fatal(err)
				}
			}
			if r := historyLocalHTTP(router, "/history/fence", request); r.Code != 200 {
				t.Fatal(r.Body.String())
			}
			r := historyLocalHTTP(router, "/history/export", request)
			if mode == "stream_no" || mode == "large_stream_no" || mode == "stream_setting" {
				if r.Code != 422 {
					t.Fatal(r.Code, r.Body.String())
				}
				return
			}
			if r.Code != 200 {
				t.Fatal(r.Body.String())
			}
			var page historyResponse
			if err := json.Unmarshal(r.Body.Bytes(), &page); err != nil {
				t.Fatal(err)
			}
			if len(page.Messages) != 1 {
				t.Fatal(page)
			}
			out := page.Messages[0]
			if out.ChannelID != msg.ChannelID || out.FromUID != msg.FromUID || out.MessageId != msg.MessageID || out.MessageSeq != 1 || out.Timestamp != msg.Timestamp || out.Expire != msg.Expire || out.Setting != msg.Setting.Uint8() ||
				out.Header.NoPersist != 1 || out.Header.RedDot != 1 || out.Header.SyncOnce != 1 || !bytes.Equal(out.Payload, msg.Payload) {
				t.Fatal(out)
			}
			if mode == "empty_tail" {
				if page.NextSeq != 3 || page.More {
					t.Fatal(page)
				}
				request.AfterSeq = 1
				r = historyLocalHTTP(router, "/history/export", request)
				if err := json.Unmarshal(r.Body.Bytes(), &page); err != nil {
					t.Fatal(err)
				}
				if len(page.Messages) != 0 || page.NextSeq != 3 || page.More {
					t.Fatal(page)
				}
			}
		})
	}
}

func TestHistoryHTTPEmptyMissingChannelAndUnfrozenCancellation(t *testing.T) {
	_, db, request := historyAPIFixture(t)
	slot := &historyAPISlot{}
	topologyCalls := 0
	s := store.New(store.NewOptions(store.WithDB(db), store.WithSlot(slot),
		store.WithHistoryTopology(func(string, uint8) error {
			topologyCalls++
			return wkdb.ErrHistoryChannelMissing
		}),
		store.WithHistoryDrain(func(context.Context, string, uint8) error { return nil })))
	slot.store = s
	router := wkhttp.New()
	(&historyAPI{store: s}).route(router)
	for i := 0; i < 2; i++ {
		if r := historyLocalHTTP(router, "/history/fence/release", request); r.Code != 200 {
			t.Fatal(r.Code, r.Body.String())
		}
	}
	if topologyCalls != 0 {
		t.Fatal("未冻结取消仍依赖缺失频道 leader")
	}
	for i := 0; i < 2; i++ {
		r := historyLocalHTTP(router, "/history/fence", request)
		var fence wkdb.HistoryFence
		if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &fence) != nil || fence.HighWatermark != 0 || fence.ChannelID != "a@c" {
			t.Fatal(r.Code, r.Body.String())
		}
	}
	r := historyLocalHTTP(router, "/history/export", request)
	var page historyResponse
	if r.Code != 200 || json.Unmarshal(r.Body.Bytes(), &page) != nil || page.NextSeq != 0 || page.HighWatermark != 0 || page.More || len(page.Messages) != 0 || page.SourceChannel != "a@c" {
		t.Fatal(r.Code, r.Body.String())
	}
	if _, err := db.GetChannelClusterConfig("a@c", 1); !errors.Is(err, wkdb.ErrNotFound) {
		t.Fatal("空迁移创建了普通频道", err)
	}
	for i := 0; i < 2; i++ {
		if r := historyLocalHTTP(router, "/history/fence/release", request); r.Code != 200 {
			t.Fatal(r.Code, r.Body.String())
		}
	}
}
