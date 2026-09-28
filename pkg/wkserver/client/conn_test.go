package client

import (
	"runtime"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"github.com/panjf2000/gnet/v2"
	"go.uber.org/atomic"
)

func TestConnTickConcurrentActivity(t *testing.T) {
	c, _ := newHeartbeatTestClient()
	c.opts.HeartbeatTick = 1 << 30
	c.opts.HeartbeatTimeoutTick = 1 << 30
	event := &clientEvent{c: c}
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		<-start
		for range 1000 {
			c.conn().tick()
			runtime.Gosched()
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for range 1000 {
			event.OnTraffic(c.conn().gc)
			runtime.Gosched()
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for range 1000 {
			c.handleData(nil, proto.MsgTypeHeartbeat, "")
			runtime.Gosched()
		}
	}()
	close(start)
	workers.Wait()
}

func TestConnTickActivityThresholds(t *testing.T) {
	tests := []struct {
		name            string
		activity        func(*Client)
		expectedTimeout int64
	}{
		{
			name: "traffic resets idle only",
			activity: func(c *Client) {
				(&clientEvent{c: c}).OnTraffic(c.conn().gc)
			},
			expectedTimeout: 1,
		},
		{
			name: "decoded data resets idle and timeout",
			activity: func(c *Client) {
				c.handleData(nil, proto.MsgTypeHeartbeat, "")
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, gc := newHeartbeatTestClient()
			c.opts.HeartbeatTick = 2
			c.opts.HeartbeatTimeoutTick = 4
			c.conn().tick()
			if gc.writes.Load() != 0 || gc.closes.Load() != 0 {
				t.Fatal("未到阈值就发送心跳或关闭连接")
			}
			c.conn().tick()
			if gc.writes.Load() != 1 {
				t.Fatal("达到原心跳阈值时未发送心跳")
			}
			tt.activity(c)
			c.conn().tick()
			if gc.writes.Load() != 1 {
				t.Fatal("收到数据后空闲计数未重置")
			}
			c.conn().tick()
			if gc.writes.Load() != 2 || gc.closes.Load() != tt.expectedTimeout {
				t.Fatalf("心跳或超时语义改变：心跳=%d，关闭=%d", gc.writes.Load(), gc.closes.Load())
			}
			if tt.expectedTimeout == 0 {
				c.conn().tick()
				c.conn().tick()
				if gc.closes.Load() != 1 {
					t.Fatal("完整数据之后未按原超时阈值关闭连接")
				}
			}
		})
	}
}

// 只构造心跳状态与连接替身，不启动事件循环或占用端口。
func newHeartbeatTestClient() (*Client, *heartbeatTestConn) {
	c := &Client{opts: NewOptions(), proto: proto.New()}
	gc := &heartbeatTestConn{}
	cn := newConn("", c)
	cn.gc = gc
	c.conns = []*conn{cn}
	return c, gc
}

type heartbeatTestConn struct {
	gnet.Conn
	writes atomic.Int64
	closes atomic.Int64
}

func (c *heartbeatTestConn) AsyncWrite([]byte, gnet.AsyncCallback) error {
	c.writes.Inc()
	return nil
}

func (c *heartbeatTestConn) Close() error {
	c.closes.Inc()
	return nil
}

func (c *heartbeatTestConn) InboundBuffered() int { return 0 }
