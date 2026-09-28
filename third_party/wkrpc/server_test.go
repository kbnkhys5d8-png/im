package wkrpc

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/wklog"
	"github.com/panjf2000/gnet/v2"
)

func TestServerStopRejectsLateBoot(t *testing.T) {
	opts := wklog.NewOptions()
	opts.LogDir = t.TempDir()
	wklog.Configure(opts)
	s := New("tcp://127.0.0.1:0")
	s.Stop()
	// 停止先发生时，晚到的启动回调必须终止引擎，不能重新开始监听。
	if action := s.OnBoot(gnet.Engine{}); action != gnet.Shutdown {
		t.Fatalf("late boot action = %v, want Shutdown", action)
	}
	if err := s.Start(); err == nil {
		t.Fatal("a stopped server accepted Start")
	}
	s.Stop()
}

func TestServerStopDuringStartup(t *testing.T) {
	opts := wklog.NewOptions()
	opts.LogDir = t.TempDir()
	wklog.Configure(opts)
	s := New("tcp://127.0.0.1:0")
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	// 不等待 OnBoot，覆盖异步启动窗口和多个关闭方；退出后不能遗留监听任务。
	var pending sync.WaitGroup
	for i := 0; i < 8; i++ {
		pending.Add(1)
		go func() {
			defer pending.Done()
			s.Stop()
		}()
	}
	pending.Wait()
	select {
	case <-s.runDone:
	default:
		t.Fatal("Stop returned before the engine exited")
	}
}

func TestServerStartStopClosesListener(t *testing.T) {
	opts := wklog.NewOptions()
	opts.LogDir = t.TempDir()
	wklog.Configure(opts)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	s := New("tcp://" + addr)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	// 用真实监听证明启动已发生，不靠固定睡眠建立并发顺序。
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			s.Stop()
			t.Fatalf("listener did not start: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	s.Stop()
	listener, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("listener remained occupied after Stop: %v", err)
	}
	_ = listener.Close()
}
