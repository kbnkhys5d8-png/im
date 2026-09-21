//go:build (linux || darwin || freebsd || dragonfly) && !poll_opt && integration

package wknet

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap/zapcore"
)

func waitForShutdownPoller(t *testing.T, minimum int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		buffer := make([]byte, 128*1024)
		used := runtime.Stack(buffer, true)
		count := 0
		for _, stack := range strings.Split(string(buffer[:used]), "\n\n") {
			if strings.Contains(stack, "netpoll.(*Poller).Polling") &&
				(strings.Contains(stack, "unix.EpollWait") || strings.Contains(stack, "unix.Kevent")) {
				count++
			}
		}
		if count >= minimum {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("polling did not block\n%s", buffer[:used])
		}
		time.Sleep(time.Millisecond)
	}
}

func awaitNetworkStop(t *testing.T, stop func() error) {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- stop() }()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		buffer := make([]byte, 128*1024)
		used := runtime.Stack(buffer, true)
		t.Fatalf("network shutdown timed out\n%s", buffer[:used])
	}
}

func TestReactorSubStopWithoutConnections(t *testing.T) {
	wklog.Configure(&wklog.Options{LogDir: t.TempDir(), Level: zapcore.ErrorLevel, NoStdout: true})
	// 空子reactor不监听端口，只验证等待中的事件循环可以退出。
	engine := &Engine{options: NewOptions(), connMatrix: newConnMatrix(), eventHandler: NewEventHandler()}
	reactor := NewReactorSub(engine, 0)
	if err := reactor.Start(); err != nil {
		t.Fatal(err)
	}
	waitForShutdownPoller(t, 1)
	awaitNetworkStop(t, reactor.Stop)
}

func TestEngineStopWithoutConnections(t *testing.T) {
	wklog.Configure(&wklog.Options{LogDir: t.TempDir(), Level: zapcore.ErrorLevel, NoStdout: true})
	// 仅监听本机临时回环端口，无客户端连接、业务目录或外部网络访问。
	engine := NewEngine(WithAddr("tcp://127.0.0.1:0"), WithWSAddr(""), WithWSSAddr(""), WithSubReactorNum(1))
	if err := engine.Start(); err != nil {
		t.Fatal(err)
	}
	waitForShutdownPoller(t, 2)
	if engine.ConnCount() != 0 {
		t.Fatal("test requires zero client connections")
	}
	awaitNetworkStop(t, engine.Stop)
}
