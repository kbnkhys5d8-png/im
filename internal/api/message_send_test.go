package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wkcache"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

type textLimitChannel struct {
	events []*eventbus.Event
}

func (c *textLimitChannel) AddEvent(_ string, _ uint8, event *eventbus.Event) {
	c.events = append(c.events, event)
}

func (c *textLimitChannel) Advance(string, uint8) {}

func TestHTTPSendRejectsOverlongTextBeforeEnqueue(t *testing.T) {
	previousOptions, previousChannel := options.G, eventbus.Channel
	options.G = options.New()
	channel := &textLimitChannel{}
	eventbus.RegisterChannel(channel)
	t.Cleanup(func() {
		options.G, eventbus.Channel = previousOptions, previousChannel
	})
	payload, err := json.Marshal(map[string]any{"type": 1, "content": strings.Repeat("文", 5001)})
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"/message/send", "/message/sendbatch"} {
		t.Run(route, func(t *testing.T) {
			channel.events = nil
			body, err := json.Marshal(map[string]any{
				"from_uid": "sender", "channel_id": "group", "channel_type": 2,
				"subscribers": []string{"receiver"}, "payload": payload,
			})
			if err != nil {
				t.Fatal(err)
			}
			router := wkhttp.New()
			m := newMessage(nil)
			if route == "/message/send" {
				// 普通发送不同时携带频道和订阅者，避免其他校验遮蔽长度检查。
				body, _ = json.Marshal(messageSendReq{FromUID: "sender", ChannelID: "group", ChannelType: 2, Payload: payload})
				router.POST(route, m.send)
			} else {
				router.POST(route, m.sendBatch)
			}
			response := httptest.NewRecorder()
			router.GetGinRoute().ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, bytes.NewReader(body)))
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400", response.Code)
			}
			if len(channel.events) != 0 {
				t.Fatalf("rejected text enqueued %d event(s)", len(channel.events))
			}
		})
	}
}

func TestSendMessageToChannelTextRuneLimit(t *testing.T) {
	previousOptions, previousChannel := options.G, eventbus.Channel
	options.G = options.New()
	config := viper.New()
	config.Set("rootDir", t.TempDir())
	config.Set("jwt.secret", "unit-test-only")
	options.G.ConfigureWithViper(config)
	channel := &textLimitChannel{}
	eventbus.RegisterChannel(channel)
	t.Cleanup(func() { options.G, eventbus.Channel = previousOptions, previousChannel })
	for _, count := range []int{5000, 5001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			channel.events = nil
			payload, _ := json.Marshal(map[string]any{"type": 1, "content": strings.Repeat("😀", count)})
			_, err := sendMessageToChannel(messageSendReq{FromUID: "sender", Payload: payload}, "group", wkproto.ChannelTypeGroup, "client")
			if count == 5000 {
				if err != nil || len(channel.events) != 1 {
					t.Fatalf("legal text error = %v, events = %d", err, len(channel.events))
				}
			} else if err == nil || len(channel.events) != 0 {
				t.Fatalf("overlong text error = %v, events = %d", err, len(channel.events))
			}
		})
	}
}

func TestHTTPSendAllowsTextBoundaryAndDoesNotLimitMedia(t *testing.T) {
	previousOptions, previousChannel := options.G, eventbus.Channel
	options.G = options.New()
	config := viper.New()
	config.Set("rootDir", t.TempDir())
	config.Set("jwt.secret", "unit-test-only")
	options.G.ConfigureWithViper(config)
	t.Cleanup(func() { options.G, eventbus.Channel = previousOptions, previousChannel })
	for _, contentType := range []int{1, 2, 3, 4, 9, 10, 1000} {
		for _, route := range []string{"/message/send", "/message/sendbatch"} {
			t.Run(fmt.Sprintf("%d%s", contentType, route), func(t *testing.T) {
				channel := &textLimitChannel{}
				eventbus.RegisterChannel(channel)
				count := 5001
				if contentType == 1 {
					count = 5000
				}
				payload, _ := json.Marshal(map[string]any{"type": contentType, "content": strings.Repeat("😀", count)})
				body, _ := json.Marshal(map[string]any{"from_uid": "sender", "subscribers": []string{"peer"}, "payload": payload})
				router := wkhttp.New()
				m := newMessage(nil)
				if route == "/message/send" {
					body, _ = json.Marshal(messageSendReq{FromUID: "sender", ChannelID: "group", ChannelType: 2, Payload: payload})
					router.POST(route, m.send)
				} else {
					router.POST(route, m.sendBatch)
				}
				response := httptest.NewRecorder()
				router.GetGinRoute().ServeHTTP(response, httptest.NewRequest(http.MethodPost, route, bytes.NewReader(body)))
				if response.Code != http.StatusOK || len(channel.events) != 1 {
					t.Fatalf("合法消息状态 %d，入队 %d 次", response.Code, len(channel.events))
				}
			})
		}
	}
}

func TestHandleStreamPersistRejectsFinalTextBeforeStoreAndDoesNotSwallowCacheError(t *testing.T) {
	previousStore, previousCache := service.Store, service.MessageEventCache
	service.Store = nil
	t.Cleanup(func() { service.Store, service.MessageEventCache = previousStore, previousCache })
	for _, useCache := range []bool{false, true} {
		t.Run(fmt.Sprintf("缓存%v", useCache), func(t *testing.T) {
			service.MessageEventCache = nil
			if useCache {
				cache := wkcache.NewMessageEventCache(nil)
				defer cache.Close()
				if _, err := cache.UpsertSession(wkcache.MessageEventSessionMeta{ClientMsgNo: "client", ChannelId: "group", ChannelType: 2}, "main", 0); err != nil {
					t.Fatal(err)
				}
				service.MessageEventCache = cache
			}
			payload, _ := json.Marshal(map[string]any{"snapshot": map[string]any{"kind": "text", "text": strings.Repeat("中", 5001)}})
			response := httptest.NewRecorder()
			context, _ := gin.CreateTestContext(response)
			m := newMessage(nil)
			// Store 为 nil：如果校验错误被吞掉并进入持久层，测试会直接失败。
			m.handleStreamPersist(&wkhttp.Context{Context: context}, &eventAppendReq{
				ClientMsgNo: "client", EventID: "bad", Payload: payload, ChannelType: 2,
			}, "group", "main", wkdb.EventTypeStreamClose)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("超限终态返回 %d，应该为 400", response.Code)
			}
		})
	}
}
