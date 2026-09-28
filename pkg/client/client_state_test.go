package client

import (
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// 空读与关闭并发，复现接收循环在网络读之间读取关闭状态的真实路径。
func TestClientReadLoopConcurrentClose(t *testing.T) {
	local, remote := net.Pipe()
	defer remote.Close()
	conn := &emptyReadConn{Conn: local, started: make(chan struct{})}
	c := New("")
	c.setup()
	c.bindToNewConn(conn)
	c.conn = conn
	c.status = CONNECTED
	c.wg.Add(1)
	go c.readLoop()
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		c.Close()
		c.wg.Wait()
		t.Fatal("接收循环未开始读取")
	}
	c.Close()
	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("关闭后接收循环未退出")
	}
}

func TestClientIsConnectedConcurrentClose(t *testing.T) {
	c := New("")
	c.status = CONNECTED
	start := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		<-start
		for range 1000 {
			c.IsConnected()
			runtime.Gosched()
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		for range 1000 {
			c.Close()
			runtime.Gosched()
		}
	}()
	close(start)
	workers.Wait()
	if c.IsConnected() {
		t.Fatal("已关闭客户端仍报告连接中")
	}
}

type emptyReadConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *emptyReadConn) Read([]byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	runtime.Gosched()
	return 0, nil
}

func (c *emptyReadConn) Write(data []byte) (int, error) {
	// 状态测试不交换数据，避免 net.Pipe 的空写阻塞关闭路径。
	return len(data), nil
}
