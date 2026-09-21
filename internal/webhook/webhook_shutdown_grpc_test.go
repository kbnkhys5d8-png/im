package webhook

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/grpcpool"
	"github.com/WuKongIM/WuKongIM/pkg/wkhook"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type blockingNotifyServer struct {
	wkhook.UnimplementedWebhookServiceServer
	entered chan struct{}
}

func (s *blockingNotifyServer) SendWebhook(ctx context.Context, _ *wkhook.EventReq) (*wkhook.EventResp, error) {
	close(s.entered)
	<-ctx.Done()
	return nil, status.FromContextError(ctx.Err()).Err()
}

func TestStopCancelsGRPCAndRetainsUnconfirmedMessage(t *testing.T) {
	// 通过纯内存连接覆盖真实 gRPC 序列化和取消，不监听端口或读取生产配置。
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = listener.Close() })
	rpcServer := grpc.NewServer()
	handler := &blockingNotifyServer{entered: make(chan struct{})}
	wkhook.RegisterWebhookServiceServer(rpcServer, handler)
	t.Cleanup(rpcServer.Stop)
	go func() { _ = rpcServer.Serve(listener) }()
	pool, err := grpcpool.New(func() (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough:///notify-test",
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return listener.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}, 1, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	w, store := newReliabilityWebhook(t, func(req *http.Request) (*http.Response, error) {
		t.Error("gRPC 用例不应请求 HTTP")
		return notifyResponse(req, http.StatusServiceUnavailable), nil
	})
	options.G.Webhook.GRPCAddr = "memory-test"
	options.G.Webhook.MsgNotifyEventCountPerPush = 1
	w.webhookGRPCPool = pool
	queue := &notifyQueueStub{data: make(chan []byte, 1)}
	w.backend, store.queue = queue, queue
	message := notifyTestMessage(61)
	data, err := message.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	queue.data <- data
	startNotifyTestLoop(w)
	select {
	case <-handler.entered:
	case <-time.After(time.Second):
		t.Fatal("gRPC 请求未进入等待")
	}
	done := make(chan error, 1)
	go func() { done <- w.Stop() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("gRPC 未随停止及时取消")
	}
	if exists, _ := store.Has(61); !exists || !queue.closed.Load() {
		t.Fatal("gRPC 结果不确定的消息未在队列关闭前保留")
	}
}
