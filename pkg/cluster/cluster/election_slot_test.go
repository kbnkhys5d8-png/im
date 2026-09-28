package cluster

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/client"
)

func TestRequestSlotInfosConcurrentResults(t *testing.T) {
	// 在启动网络协程前完成日志器的惰性初始化。
	wklog.Info("初始化槽日志并发收集测试")
	for _, tc := range []struct {
		name       string
		failedNode uint64
	}{
		{name: "all_success"},
		{name: "one_remote_failure", failedNode: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const remoteCount = 16
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := "tcp://" + listener.Addr().String()
			if err := listener.Close(); err != nil {
				t.Fatal(err)
			}
			requested := make(chan struct{}, remoteCount)
			release := make(chan struct{})
			var releaseOnce sync.Once
			remote := wkserver.New(addr)
			remote.Route("/rpc/slot/lastLogInfo", func(c *wkserver.Context) {
				var req SlotLogInfoReq
				if err := req.Unmarshal(c.Body()); err != nil {
					c.WriteErr(err)
					return
				}
				requested <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return
				}
				nodeID := uint64(req.SlotIds[0])
				if nodeID == tc.failedNode {
					c.WriteErr(errors.New("测试远程节点失败"))
					return
				}
				resp := &SlotLogInfoResp{NodeId: nodeID, Slots: []SlotInfo{{SlotId: req.SlotIds[0], LogIndex: nodeID}}}
				data, err := resp.Marshal()
				if err != nil {
					c.WriteErr(err)
					return
				}
				c.Write(data)
			})
			if err := remote.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(remote.Stop)
			cli := client.New(addr, client.WithUid("slot-info-test"))
			if err := cli.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cli.Stop)
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
			ticker := time.NewTicker(time.Millisecond * 10)
			defer ticker.Stop()
			for !cli.IsAuthed() {
				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			opts := NewOptions(WithConfigOptions(clusterconfig.NewOptions(clusterconfig.WithNodeId(1))))
			s := &Server{opts: opts, nodeManager: newNodeManager(opts), Log: wklog.NewWKLog("slot-info-test")}
			s.rpcClient = newRpcClient(s)
			// 本地空槽无需启动存储，远程节点复用真实 RPC 客户端验证结果收集。
			slots := map[uint64][]uint32{1: {}}
			for id := uint64(2); id <= remoteCount+1; id++ {
				s.nodeManager.addNode(&ImprovedNode{id: id, client: cli})
				slots[id] = []uint32{uint32(id)}
			}
			type result struct {
				resps []*SlotLogInfoResp
				err   error
			}
			done := make(chan result, 1)
			go func() {
				resps, err := s.requestSlotInfos(slots)
				done <- result{resps: resps, err: err}
			}()
			// 聚齐请求后同时放行，让多个成功响应争用同一结果收集路径。
			for i := 0; i < remoteCount; i++ {
				select {
				case <-requested:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case got := <-done:
				if got.err != nil {
					t.Fatal(got.err)
				}
				seen := make(map[uint64]bool)
				for _, resp := range got.resps {
					if resp == nil || seen[resp.NodeId] {
						t.Fatalf("收到空结果或重复节点结果: %+v", resp)
					}
					seen[resp.NodeId] = true
				}
				for id := range slots {
					if seen[id] != (id != tc.failedNode) {
						t.Errorf("节点 %d 的结果存在=%v, 失败节点=%d", id, seen[id], tc.failedNode)
					}
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
