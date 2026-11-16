//go:build linux && !poll_opt

package wknet

import (
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

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
