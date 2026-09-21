// Copyright 2023 tangtao. All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

//go:build linux && !poll_opt
// +build linux,!poll_opt

package netpoll

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

type Poller struct {
	wklog.Log
	fd    int
	efd   int
	state atomic.Int32
	mu    sync.Mutex
	name  string
}

const (
	pollerIdle int32 = iota
	pollerRunning
	pollerClosed
)

func NewPoller(index int, name string) *Poller {
	var (
		err    error
		poller = new(Poller)
	)
	poller.name = name
	poller.fd, err = unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		panic(err)
	}
	poller.efd, err = unix.Eventfd(0, unix.EFD_NONBLOCK|unix.EFD_CLOEXEC)
	if err != nil {
		unix.Close(poller.fd)
		panic(err)
	}
	poller.Log = wklog.NewWKLog(fmt.Sprintf("epollPoller[%d]", index))

	err = poller.AddRead(poller.efd)
	if err != nil {
		_ = poller.Close()
		panic(err)
	}

	return poller
}

// Polling blocks the current goroutine, waiting for network-events.
func (p *Poller) Polling(callback func(fd int, event PollEvent) error) (retErr error) {
	p.mu.Lock()
	switch p.state.Load() {
	case pollerClosed:
		p.mu.Unlock()
		return nil
	case pollerRunning:
		p.mu.Unlock()
		return errors.New("poller is already running")
	}
	p.state.Store(pollerRunning)
	p.mu.Unlock()
	// 已启动的轮询独占最终回收；Close 只唤醒，不等待回调自身退出。
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.state.Store(pollerClosed)
		retErr = errors.Join(retErr, p.closeFDsLocked())
	}()

	el := newEventList(InitPollEventsCap)
	msec := -1
	for p.state.Load() != pollerClosed {
		n, err := unix.EpollWait(p.fd, el.events, msec)
		if n == 0 || (n < 0 && err == unix.EINTR) {
			msec = -1
			runtime.Gosched()
			continue
		} else if err != nil {
			p.Error("error occurs in epoll", zap.Error(err))
			return err
		}
		msec = 0

		var triggerRead, triggerWrite, triggerHup, triggerError bool
		var pollEvent PollEvent

		// test := make([]byte, 10000)
		for i := 0; i < n; i++ {
			if p.state.Load() == pollerClosed {
				return nil
			}
			evt := el.events[i]
			fd := evt.Fd
			// eventfd 仅用于内部唤醒，不能交给连接或 accept 回调。
			if int(fd) == p.efd {
				continue
			}
			pollEvent = PollEventUnknown
			triggerRead = evt.Events&readEvents != 0
			triggerWrite = evt.Events&unix.EPOLLOUT != 0
			triggerHup = evt.Events&(unix.EPOLLHUP|unix.EPOLLRDHUP) != 0
			triggerError = evt.Events&unix.EPOLLERR != 0

			if triggerRead {
				pollEvent = PollEventRead
			} else if triggerHup || triggerError {
				pollEvent = PollEventClose
			}
			if pollEvent != PollEventUnknown {
				switch err = callback(int(fd), pollEvent); err {
				case nil:
				default:
					p.Error("error occurs in event-loop", zap.Error(err))
				}
			}
			if p.state.Load() == pollerClosed {
				return nil
			}
			if triggerWrite && !(triggerHup || triggerError) {
				switch err = callback(int(fd), PollEventWrite); err {
				case nil:
				default:
					p.Error("error occurs in event-loop", zap.Error(err))
				}
			}
		}
		if n == el.size {
			el.expand()
		} else if n < el.size>>1 {
			el.shrink()
		}
	}
	return nil
}

const (
	readEvents      = unix.EPOLLPRI | unix.EPOLLIN
	writeEvents     = unix.EPOLLOUT
	readWriteEvents = readEvents | writeEvents
	errorEvents     = unix.EPOLLERR | unix.EPOLLHUP | unix.EPOLLRDHUP
)

// AddRead registers the given file-descriptor with readable event to the poller.
func (p *Poller) AddRead(fd int) error {
	return os.NewSyscallError("epoll_ctl add",
		p.control(unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{Fd: int32(fd), Events: readEvents}))
}

func (p *Poller) AddWrite(fd int) error {
	return os.NewSyscallError("epoll_ctl add",
		p.control(unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{Fd: int32(fd), Events: readWriteEvents}))
}

// DeleteRead deletes the given file-descriptor from the poller.
func (p *Poller) DeleteRead(fd int) error {
	return os.NewSyscallError("epoll_ctl delete",
		p.control(unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{Fd: int32(fd), Events: writeEvents}))
}

// DeleteWrite deletes the given file-descriptor from the poller.
func (p *Poller) DeleteWrite(fd int) error {
	return os.NewSyscallError("epoll_ctl delete",
		p.control(unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{Fd: int32(fd), Events: readEvents}))
}

func (p *Poller) DeleteReadAndWrite(fd int) error {
	return os.NewSyscallError("epoll_ctl delete",
		p.control(unix.EPOLL_CTL_DEL, fd, &unix.EpollEvent{Fd: int32(fd), Events: readWriteEvents}))
}

func (p *Poller) Delete(fd int) error {
	return p.control(unix.EPOLL_CTL_DEL, fd, nil)
}

func (p *Poller) control(operation, fd int, event *unix.EpollEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 与回收使用同一把锁，禁止旧 poller 操作已经复用的 epoll fd。
	if p.state.Load() == pollerClosed {
		return os.ErrClosed
	}
	return unix.EpollCtl(p.fd, operation, fd, event)
}

// Close closes the poller.
func (p *Poller) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	state := p.state.Load()
	if state == pollerClosed {
		return nil
	}
	p.state.Store(pollerClosed)
	if state == pollerIdle {
		// 没有启动 Polling 时不会有轮询协程负责回收。
		return p.closeFDsLocked()
	}

	// 关闭 eventfd 无法唤醒空 EpollWait，写入计数后由轮询协程统一关闭两个 fd。
	var value [8]byte
	binary.NativeEndian.PutUint64(value[:], 1)
	for {
		n, err := unix.Write(p.efd, value[:])
		if err == unix.EINTR {
			continue
		}
		if err == unix.EAGAIN {
			// 计数器已满时 eventfd 已可读，仍然可以唤醒轮询。
			return nil
		}
		if err == nil && n != len(value) {
			err = io.ErrShortWrite
		}
		return os.NewSyscallError("eventfd write", err)
	}
}

func (p *Poller) closeFDsLocked() error {
	// 调用方持有生命周期锁，且只由未启动的 Close 或 Polling 的 defer 调用一次。
	// Linux close 出错也不能重试，以免关闭其他协程刚复用的 fd。
	return errors.Join(
		os.NewSyscallError("close epoll", unix.Close(p.fd)),
		os.NewSyscallError("close eventfd", unix.Close(p.efd)),
	)
}
