package webhook

import (
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

// 验证未获得接口成功响应的通知不能因重试次数耗尽而消失。
func TestPushMessagesRetainsUnacknowledgedAfterRetryLimit(t *testing.T) {
	previous := options.G
	options.G = &options.Options{}
	options.G.Webhook.HTTPAddr = "http://notify.invalid/webhook"
	options.G.Webhook.MsgNotifyEventRetryMaxCount = 3
	t.Cleanup(func() { options.G = previous })

	attempts := 0
	failureDir := filepath.Join(t.TempDir(), "failed")
	store, err := newNotifyFailureStore(failureDir)
	if err != nil {
		t.Fatal(err)
	}
	w := &Webhook{
		Log: silentWebhookLog{}, failures: store,
		httpClient: &http.Client{Transport: notifyRoundTripper(func(req *http.Request) (*http.Response, error) {
			attempts++
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Body:       io.NopCloser(strings.NewReader("")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		})},
	}
	message := wkdb.Message{RecvPacket: wkproto.RecvPacket{
		MessageID: 1, MessageSeq: 1, ChannelID: "notify-test", ChannelType: wkproto.ChannelTypeGroup,
		Payload: []byte(`{"type":1,"content":"test"}`),
	}}
	pending := []wkdb.Message{message}
	failures := make(map[int64]int)
	for i := 1; i <= options.G.Webhook.MsgNotifyEventRetryMaxCount; i++ {
		allSucceeded, remaining := w.pushMessages(pending, failures)
		exists, err := store.Has(message.MessageID)
		if allSucceeded || err != nil || !exists {
			t.Fatalf("第 %d 次 HTTP 503 后未保留通知：allSucceeded=%v retained=%v error=%v", i, allSucceeded, exists, err)
		}
		pending = remaining
	}
	if len(pending) != 0 || attempts != 3 {
		t.Fatal("达到上限后应该暂停自动重试")
	}
	// 新实例只核对已有证据，不将失败记录重新放回自动投递队列。
	reopened, err := newNotifyFailureStore(failureDir)
	if err != nil {
		t.Fatal(err)
	}
	w.failures = reopened
	w.pushMessages([]wkdb.Message{message}, make(map[int64]int))
	if attempts != 3 {
		t.Fatal("重启后自动重放了失败通知")
	}
}

type notifyRoundTripper func(*http.Request) (*http.Response, error)

func (f notifyRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// 仅屏蔽复现路径的日志，避免测试创建工作目录日志文件。
type silentWebhookLog struct{ wklog.Log }

func (silentWebhookLog) Info(string, ...zap.Field)  {}
func (silentWebhookLog) Debug(string, ...zap.Field) {}
func (silentWebhookLog) Warn(string, ...zap.Field)  {}
func (silentWebhookLog) Error(string, ...zap.Field) {}
