//go:build linux && !poll_opt

package netpoll

import (
	"errors"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func awaitPollerExit(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Polling returned an error: %v", err)
		}
	case <-time.After(2 * time.Second):
		buffer := make([]byte, 256*1024)
		used := runtime.Stack(buffer, true)
		t.Fatalf("poller did not stop\n%s", buffer[:used])
	}
}

func startIdlePoller(t *testing.T, p *Poller) (<-chan error, *atomic.Int32) {
	t.Helper()
	callbacks := new(atomic.Int32)
	result := make(chan error, 1)
	go func() {
		result <- p.Polling(func(int, PollEvent) error {
			callbacks.Add(1)
			return nil
		})
	}()
	// 确認轮询已进入阻塞系统调用，避免仅覆盖 Close 早于 Polling 的路径。
	deadline := time.Now().Add(2 * time.Second)
	for {
		buffer := make([]byte, 64*1024)
		used := runtime.Stack(buffer, true)
		for _, stack := range strings.Split(string(buffer[:used]), "\n\n") {
			if strings.Contains(stack, "unix.EpollWait") && strings.Contains(stack, "netpoll.(*Poller).Polling") {
				return result, callbacks
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("poller did not enter epoll_wait")
		}
		time.Sleep(time.Millisecond)
	}
}

func assertPollerFDsClosed(t *testing.T, p *Poller) {
	t.Helper()
	for _, fd := range []int{p.fd, p.efd} {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
			t.Fatalf("poller still owns descriptor %d: %v", fd, err)
		}
	}
}

func TestPollerCloseBeforePolling(t *testing.T) {
	p := NewPoller(0, t.Name())
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	assertPollerFDsClosed(t, p)
	result := make(chan error, 1)
	go func() { result <- p.Polling(func(int, PollEvent) error { return nil }) }()
	awaitPollerExit(t, result)
	if err := p.Close(); err != nil {
		t.Fatalf("repeated Close must be harmless: %v", err)
	}
}

func TestPollerCloseWakesIdlePolling(t *testing.T) {
	p := NewPoller(0, t.Name())
	result, callbacks := startIdlePoller(t, p)
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	awaitPollerExit(t, result)
	if callbacks.Load() != 0 {
		t.Fatal("internal wake event reached the business callback")
	}
	assertPollerFDsClosed(t, p)
}

func TestPollerConcurrentClose(t *testing.T) {
	p := NewPoller(0, t.Name())
	result, callbacks := startIdlePoller(t, p)
	closeErrors := make(chan error, 32)
	var callers sync.WaitGroup
	for i := 0; i < cap(closeErrors); i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			closeErrors <- p.Close()
		}()
	}
	callers.Wait()
	close(closeErrors)
	for err := range closeErrors {
		if err != nil {
			t.Fatalf("concurrent Close: %v", err)
		}
	}
	awaitPollerExit(t, result)
	if callbacks.Load() != 0 {
		t.Fatal("internal wake event reached the business callback")
	}
	assertPollerFDsClosed(t, p)
}

func TestPollerStartCloseRace(t *testing.T) {
	for i := 0; i < 64; i++ {
		p := NewPoller(i, t.Name())
		start := make(chan struct{})
		polling := make(chan error, 1)
		closing := make(chan error, 1)
		go func() {
			<-start
			polling <- p.Polling(func(int, PollEvent) error { return nil })
		}()
		go func() { <-start; closing <- p.Close() }()
		close(start)
		awaitPollerExit(t, closing)
		awaitPollerExit(t, polling)
		assertPollerFDsClosed(t, p)
	}
}

func TestPollerCloseInsideReadCallback(t *testing.T) {
	p := NewPoller(0, t.Name())
	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(sockets[0])
	defer unix.Close(sockets[1])
	if err := p.AddRead(sockets[0]); err != nil {
		t.Fatal(err)
	}
	if err := p.AddWrite(sockets[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(sockets[1], []byte{1}); err != nil {
		t.Fatal(err)
	}
	var events []PollEvent
	var callbackErr error
	result := make(chan error, 1)
	go func() {
		result <- p.Polling(func(fd int, event PollEvent) error {
			events = append(events, event)
			if event == PollEventRead {
				var value [1]byte
				_, callbackErr = unix.Read(fd, value[:])
				callbackErr = errors.Join(callbackErr, p.Close())
			}
			return nil
		})
	}()
	awaitPollerExit(t, result)
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}
	if len(events) != 1 || events[0] != PollEventRead {
		t.Fatalf("callback Close must suppress the same event's later write callback: %v", events)
	}
	assertPollerFDsClosed(t, p)
}

func TestPollerReadAndWriteCallbacks(t *testing.T) {
	p := NewPoller(0, t.Name())
	defer p.Close()
	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(sockets[0])
	defer unix.Close(sockets[1])
	if err := p.AddRead(sockets[0]); err != nil {
		t.Fatal(err)
	}
	if err := p.AddWrite(sockets[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(sockets[1], []byte{1}); err != nil {
		t.Fatal(err)
	}
	var events []PollEvent
	var callbackErr error
	result := make(chan error, 1)
	go func() {
		result <- p.Polling(func(fd int, event PollEvent) error {
			events = append(events, event)
			if event == PollEventRead {
				var value [1]byte
				_, callbackErr = unix.Read(fd, value[:])
			}
			if event == PollEventWrite {
				callbackErr = errors.Join(callbackErr, p.Close())
			}
			return nil
		})
	}()
	awaitPollerExit(t, result)
	if callbackErr != nil {
		t.Fatal(callbackErr)
	}
	// 没有在读回调中关闭时，保留原有同一事件先读后写的行为。
	if len(events) != 2 || events[0] != PollEventRead || events[1] != PollEventWrite {
		t.Fatalf("read/write callback order changed: %v", events)
	}
	assertPollerFDsClosed(t, p)
}

func TestPollerCloseKeepsFDsUntilCallbackExits(t *testing.T) {
	p := NewPoller(0, t.Name())
	defer p.Close()
	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(sockets[0])
	defer unix.Close(sockets[1])
	if err := p.AddRead(sockets[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(sockets[1], []byte{1}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	result := make(chan error, 1)
	go func() {
		result <- p.Polling(func(int, PollEvent) error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("callback did not start")
	}
	closing := make(chan error, 1)
	go func() { closing <- p.Close() }()
	awaitPollerExit(t, closing)
	// 回调退出前仍由轮询持有 fd，避免其他协程复用后轮询继续访问旧编号。
	for _, fd := range []int{p.fd, p.efd} {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
			t.Fatalf("Close reclaimed fd %d before callback exit: %v", fd, err)
		}
	}
	release <- struct{}{}
	awaitPollerExit(t, result)
	assertPollerFDsClosed(t, p)
}

func TestPollerCallbackPanicClosesOwnedFDs(t *testing.T) {
	p := NewPoller(0, t.Name())
	defer p.Close()
	sockets, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(sockets[0])
	defer unix.Close(sockets[1])
	if err := p.AddRead(sockets[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.Write(sockets[1], []byte{1}); err != nil {
		t.Fatal(err)
	}
	result := make(chan any, 1)
	go func() {
		// 仅在测试边界捕获 panic，验证 Polling 的 defer 先完成资源回收。
		defer func() { result <- recover() }()
		_ = p.Polling(func(int, PollEvent) error { panic("callback panic") })
	}()
	select {
	case value := <-result:
		if value != "callback panic" {
			t.Fatalf("unexpected panic: %v", value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("callback panic did not return")
	}
	assertPollerFDsClosed(t, p)
	if err := p.Close(); err != nil {
		t.Fatalf("Close after panic: %v", err)
	}
}

func TestPollerClosedInstanceDoesNotAffectReusedDescriptors(t *testing.T) {
	for _, started := range []bool{false, true} {
		name := "before_polling"
		if started {
			name = "after_polling"
		}
		t.Run(name, func(t *testing.T) {
			old := NewPoller(0, t.Name())
			var done <-chan error
			if started {
				done, _ = startIdlePoller(t, old)
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			if started {
				awaitPollerExit(t, done)
			}
			assertPollerFDsClosed(t, old)
			replacement := NewPoller(1, t.Name())
			defer replacement.Close()
			// 只使用内核自然分配的编号，禁止 Dup2 覆盖可能被其他协程占用的 fd。
			if replacement.fd != old.fd || replacement.efd != old.efd {
				t.Skip("kernel did not reuse both descriptors in this run")
			}
			if err := old.Close(); err != nil {
				t.Fatalf("repeated Close: %v", err)
			}
			for _, operation := range []func(int) error{old.AddRead, old.AddWrite, old.DeleteRead, old.DeleteWrite, old.DeleteReadAndWrite, old.Delete} {
				if err := operation(replacement.efd); !errors.Is(err, os.ErrClosed) {
					t.Fatalf("closed poller control must report closed: %v", err)
				}
			}
			for _, fd := range []int{replacement.fd, replacement.efd} {
				if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); err != nil {
					t.Fatalf("old Close damaged replacement fd %d: %v", fd, err)
				}
			}
			var value [8]byte
			if _, err := unix.Read(replacement.efd, value[:]); !errors.Is(err, unix.EAGAIN) {
				t.Fatalf("old Close wrote to the replacement eventfd: %v", err)
			}
		})
	}
}
