//go:build linux && !poll_opt

package wknet

import (
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/WuKongIM/WuKongIM/pkg/wknet/netpoll"
	"golang.org/x/sys/unix"
)

func TestConnConcurrentEnqueueRetainsWriteWake(t *testing.T) {
	_, reactor := isolatedReactor(t)
	conn, peer := socketConnection(t, reactor, 1)
	if err := reactor.AddConn(conn); err != nil {
		t.Fatal(err)
	}
	if err := conn.WakeWrite(); err != nil {
		t.Fatal(err)
	}
	fd := conn.Fd().Fd()
	epfd := findEpollWatching(t, fd)
	poller := reactor.poller
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	defer unblock()
	reactor.poller = &pausedDeleteWrite{reactorPoller: poller, entered: entered, resume: resume}
	flushed := make(chan error, 1)
	go func() { flushed <- conn.Flush() }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("未进入空队列取消写监听的路径")
	}
	published := make(chan error, 1)
	go func() {
		_, err := conn.WriteToOutboundBuffer([]byte("queued"))
		if err == nil {
			err = conn.WakeWrite()
		}
		published <- err
	}()
	// 暂停正在取消的旧唤醒，给并发发送者机会尝试入队并打开新监听。
	publishedEarly := false
	select {
	case err := <-published:
		publishedEarly = true
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(25 * time.Millisecond):
	}
	unblock()
	if err := <-flushed; err != nil {
		t.Fatal(err)
	}
	if !publishedEarly {
		if err := <-published; err != nil {
			t.Fatal(err)
		}
	}
	reactor.poller = poller
	if ready := readReadyEpollMask(t, epfd, fd); ready&unix.EPOLLOUT == 0 {
		t.Fatalf("新队列的写唤醒被旧回调覆盖: %#x", ready)
	}
	if err := conn.Flush(); err != nil {
		t.Fatal(err)
	}
	var out [16]byte
	if n, err := unix.Read(peer, out[:]); err != nil || n != len("queued") || string(out[:n]) != "queued" {
		t.Fatalf("唤醒后没有发出队列中的消息: n=%d err=%v", n, err)
	}
}

type pausedDeleteWrite struct {
	reactorPoller
	entered chan<- struct{}
	resume  <-chan struct{}
}

func (p *pausedDeleteWrite) DeleteWrite(fd int) error {
	close(p.entered)
	<-p.resume
	return p.reactorPoller.DeleteWrite(fd)
}

func TestConnCloseUsesOriginalWrapper(t *testing.T) {
	for _, tc := range []struct {
		name string
		wrap func(*DefaultConn) Conn
	}{
		{name: "tcp", wrap: func(c *DefaultConn) Conn { return c }},
		{name: "websocket", wrap: func(c *DefaultConn) Conn { return NewWSConn(c) }},
		{name: "tls", wrap: func(c *DefaultConn) Conn { return newTLSConn(c) }},
		{name: "wss", wrap: func(c *DefaultConn) Conn { return NewWSSConn(newTLSConn(c)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine, reactor := isolatedReactor(t)
			closed := make(chan Conn, 1)
			engine.OnClose(func(c Conn) { closed <- c })
			base, _ := socketConnection(t, reactor, 1)
			conn := tc.wrap(base)
			if err := reactor.AddConn(conn); err != nil {
				t.Fatal(err)
			}
			if err := conn.CloseWithErr(unix.ECONNRESET); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-closed:
				if got != conn {
					t.Fatalf("关闭回调丢失包装身份: got %T, want %T", got, conn)
				}
			default:
				t.Fatal("没有调用关闭回调")
			}
			if engine.ConnCount() != 0 || reactor.connCount.Load() != 0 {
				t.Fatal("关闭后仍然登记了连接")
			}
		})
	}
}

func TestConnReactorRejectsOldBatchAfterRegistration(t *testing.T) {
	engine, reactor := isolatedReactor(t)
	old, _ := socketConnection(t, reactor, 1)
	if err := reactor.AddConn(old); err != nil {
		t.Fatal(err)
	}
	oldBatch := reactor.registration.Load()
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	replacement, peer := socketConnection(t, reactor, 2)
	if err := reactor.AddConn(replacement); err != nil {
		t.Fatal(err)
	}
	// 用旧批次的代次模拟内核已取出同一 FD 的事件；不要求内核必然复用某个 FD。
	fd := replacement.Fd().Fd()
	if _, err := unix.Write(peer, []byte("incoming")); err != nil {
		t.Fatal(err)
	}
	if _, err := replacement.WriteToOutboundBuffer([]byte("outgoing")); err != nil {
		t.Fatal(err)
	}
	for _, event := range []netpoll.PollEvent{netpoll.PollEventRead, netpoll.PollEventWrite, netpoll.PollEventClose} {
		if err := reactor.handleEvent(fd, oldBatch, event); err != nil {
			t.Fatal(err)
		}
	}
	if replacement.IsClosed() || engine.ConnCount() != 1 || !replacement.inboundBuffer.IsEmpty() {
		t.Fatal("旧批次事件作用于新连接")
	}
	var out [32]byte
	if _, err := unix.Read(peer, out[:]); !errors.Is(err, unix.EAGAIN) {
		t.Fatalf("旧批次写事件错误地发送了数据: %v", err)
	}
	if _, err := old.ReadToInboundBuffer(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("旧连接仍尝试读取 FD: %v", err)
	}
	current := reactor.registration.Load()
	if err := reactor.handleEvent(fd, current, netpoll.PollEventRead); err != nil {
		t.Fatal(err)
	}
	if err := reactor.handleEvent(fd, current, netpoll.PollEventWrite); err != nil {
		t.Fatal(err)
	}
	if got, err := replacement.Peek(-1); err != nil || string(got) != "incoming" {
		t.Fatalf("新批次未能正常接收: %q %v", got, err)
	}
	if n, err := unix.Read(peer, out[:]); err != nil || n != len("outgoing") || string(out[:n]) != "outgoing" {
		t.Fatalf("新批次未能正常发送: n=%d err=%v", n, err)
	}
}

func TestConnReplacedIdleCallbackCannotClose(t *testing.T) {
	engine, reactor := isolatedReactor(t)
	engine.timingWheel = timingwheel.NewTimingWheel(10*time.Millisecond, 1000)
	conn, _ := socketConnection(t, reactor, 1)
	if err := reactor.AddConn(conn); err != nil {
		t.Fatal(err)
	}
	conn.SetMaxIdle(time.Minute)
	old := conn.idleGeneration
	conn.SetMaxIdle(2 * time.Minute)
	newTimer := conn.idleTimer
	conn.lastActivity.Store(time.Now().Add(-3 * time.Minute))
	conn.expireIdle(old, time.Minute)
	if conn.IsClosed() || conn.idleTimer != newTimer || newTimer == nil {
		t.Fatal("旧回调撤销了新定时器或关闭了当前连接")
	}
	conn.expireIdle(conn.idleGeneration, 2*time.Minute)
	if !conn.IsClosed() || conn.idleTimer != nil || engine.ConnCount() != 0 {
		t.Fatal("当前回调没有清理超时连接")
	}
}

func isolatedReactor(t *testing.T) (*Engine, *ReactorSub) {
	t.Helper()
	engine := &Engine{options: NewOptions(), eventHandler: NewEventHandler(), connMatrix: newConnMatrix()}
	reactor := NewReactorSub(engine, 0)
	t.Cleanup(func() {
		if err := reactor.Stop(); err != nil {
			t.Errorf("关闭测试轮询器: %v", err)
		}
	})
	return engine, reactor
}

func socketConnection(t *testing.T, reactor *ReactorSub, id int64) (*DefaultConn, int) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	conn := GetDefaultConn(id, newNetFd(pair[0]), nil, nil, reactor.eg, reactor)
	t.Cleanup(func() {
		_ = conn.Close()
		_ = unix.Close(pair[1])
	})
	return conn, pair[1]
}

func TestDefaultConnCloseUnregistersFromEpoll(t *testing.T) {
	for _, tc := range []struct {
		name      string
		closeErr  error
		closePeer bool
	}{
		{name: "application_close"},
		{name: "peer_reset", closeErr: unix.ECONNRESET, closePeer: true},
		{name: "wrapped_peer_reset", closeErr: os.NewSyscallError("read", unix.ECONNRESET), closePeer: true},
		{name: "other_read_error", closeErr: unix.EIO, closePeer: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, epfd, oldFD, before := newReadyEpollCloseFixture(t, tc.closeErr, tc.closePeer)
			if after := readReadyEpollMask(t, epfd, oldFD); after != 0 {
				t.Fatalf("关闭前事件=%#x，关闭后旧 FD %d 仍报告事件=%#x", before, oldFD, after)
			}
		})
	}
}

func newReadyEpollCloseFixture(t testing.TB, closeErr error, closePeer bool) (*ReactorSub, int, int, uint32) {
	t.Helper()
	engine := &Engine{options: NewOptions(), eventHandler: NewEventHandler(), connMatrix: newConnMatrix()}
	reactor := NewReactorSub(engine, 0)
	t.Cleanup(func() {
		if err := reactor.Stop(); err != nil {
			t.Errorf("关闭测试轮询器: %v", err)
		}
	})
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	peerFD := pair[1]
	t.Cleanup(func() {
		if peerFD >= 0 {
			_ = unix.Close(peerFD)
		}
	})
	conn := GetDefaultConn(1, newNetFd(pair[0]), nil, nil, engine, reactor)
	t.Cleanup(func() { _ = conn.Close() })
	if err := reactor.AddConn(conn); err != nil {
		t.Fatal(err)
	}
	epfd := findEpollWatching(t, pair[0])
	// 保留同一个 socket 的引用：仅关闭原 FD 时，内核不一定移除 epoll 注册项。
	alias, err := unix.FcntlInt(uintptr(pair[0]), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(alias) })
	if _, err := unix.Write(peerFD, []byte{1}); err != nil {
		t.Fatal(err)
	}
	if closePeer {
		// 对端有未读数据时关闭，覆盖可读、错误和挂断同时到达的情况。
		if _, err := unix.Write(alias, []byte{1}); err != nil {
			t.Fatal(err)
		}
		err := unix.Close(peerFD)
		peerFD = -1
		if err != nil {
			t.Fatal(err)
		}
	}
	before := readReadyEpollMask(t, epfd, pair[0])
	if before&unix.EPOLLIN == 0 || (closePeer && before&(unix.EPOLLERR|unix.EPOLLHUP) != unix.EPOLLERR|unix.EPOLLHUP) {
		t.Fatalf("测试连接未产生预期事件: %#x", before)
	}
	if closeErr == nil {
		err = conn.Close()
	} else {
		err = conn.CloseWithErr(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	if engine.GetConn(pair[0]) != nil || engine.ConnCount() != 0 {
		t.Fatal("旧连接仍在引擎索引中")
	}
	return reactor, epfd, pair[0], before
}

func findEpollWatching(t testing.TB, target int) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.Itoa(target)
	for _, entry := range entries {
		fd := entry.Name()
		link, err := os.Readlink("/proc/self/fd/" + fd)
		if err != nil || link != "anon_inode:[eventpoll]" {
			continue
		}
		info, err := os.ReadFile("/proc/self/fdinfo/" + fd)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(info), "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 2 && fields[0] == "tfd:" && fields[1] == want {
				epfd, err := strconv.Atoi(fd)
				if err != nil {
					t.Fatal(err)
				}
				return epfd
			}
		}
	}
	t.Fatalf("未找到测试 FD %d 的 epoll 注册项", target)
	return -1
}

func readReadyEpollMask(t testing.TB, epfd, target int) uint32 {
	t.Helper()
	var events [8]unix.EpollEvent
	for {
		n, err := unix.EpollWait(epfd, events[:], 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, event := range events[:n] {
			if int(event.Fd) == target {
				return event.Events
			}
		}
		return 0
	}
}
