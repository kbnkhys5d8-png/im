package ingress

import (
	"context"
	"errors"
	"testing"
)

func TestCanceledSessionRPCDoesNotStartNetworkRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// 不配置全局服务；取消必须在编码连接和访问集群前返回。
	client := NewClient()
	if err := client.RevokeConnContext(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("撤销未响应取消: %v", err)
	}
	if _, err := client.UserConnsContext(ctx, 1, "u"); !errors.Is(err, context.Canceled) {
		t.Fatalf("查询未响应取消: %v", err)
	}
}
