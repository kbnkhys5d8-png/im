//go:build linux || freebsd || dragonfly || darwin
// +build linux freebsd dragonfly darwin

package wknet

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wknet/netpoll"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

type reactorPoller interface {
	PollingWithSnapshot(func() uint64, func(int, uint64, netpoll.PollEvent) error) error
	AddRead(int) error
	AddWrite(int) error
	DeleteRead(int) error
	DeleteWrite(int) error
	DeleteReadAndWrite(int) error
	Delete(int) error
	Close() error
}

// ReactorSub is a sub reactor.
type ReactorSub struct {
	poller    reactorPoller
	eg        *Engine
	idx       int // index of the current sub reactor
	connCount atomic.Int32
	wklog.Log
	ReadBuffer []byte
	cache      bytes.Buffer // temporary buffer for scattered bytes

	stopped      atomic.Bool
	registration atomic.Uint64
	runWG        sync.WaitGroup
}

// NewReactorSub instantiates a sub reactor.
func NewReactorSub(eg *Engine, index int) *ReactorSub {
	poller := netpoll.NewPoller(index, "connPoller")

	return &ReactorSub{
		eg:         eg,
		poller:     poller,
		idx:        index,
		Log:        wklog.NewWKLog(fmt.Sprintf("ReactorSub-%d", index)),
		ReadBuffer: make([]byte, eg.options.ReadBufferSize),
	}
}

// AddConn adds a connection to the sub reactor.
func (r *ReactorSub) AddConn(conn Conn) error {
	if d := defaultConnOf(conn); d != nil {
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.closed.Load() {
			return net.ErrClosed
		}
		d.owner = conn
	}
	// 只有接管了底层 FD 后才分配新代次；旧批次始终早于复用后的连接。
	r.eg.addConn(conn, r.registration.Inc())
	r.connCount.Inc()
	if err := r.poller.AddRead(conn.Fd().fd); err != nil {
		r.eg.removeConn(conn)
		r.connCount.Dec()
		return err
	}
	return nil
}

// Start starts the sub reactor.
func (r *ReactorSub) Start() error {
	r.runWG.Add(1)
	go func() {
		defer r.runWG.Done()
		r.run()
	}()
	return nil
}

// Stop stops the sub reactor.
func (r *ReactorSub) Stop() error {
	r.stopped.Store(true)
	err := r.poller.Close()
	r.runWG.Wait()
	return err
}

func (r *ReactorSub) AddWrite(conn Conn) error {
	return r.poller.AddWrite(conn.Fd().fd)
}

func (r *ReactorSub) AddRead(conn Conn) error {
	return r.poller.AddRead(conn.Fd().fd)
}

func (r *ReactorSub) RemoveWrite(conn Conn) error {
	return r.poller.DeleteWrite(conn.Fd().fd)
}

func (r *ReactorSub) RemoveRead(conn Conn) error {
	return r.poller.DeleteRead(conn.Fd().fd)
}

func (r *ReactorSub) RemoveReadAndWrite(conn Conn) error {
	return r.poller.DeleteReadAndWrite(conn.Fd().fd)
}

func (r *ReactorSub) DeleteFd(conn Conn) error {
	return r.poller.Delete(conn.Fd().fd)
}

func (r *ReactorSub) ConnInc() {
	r.connCount.Inc()
}
func (r *ReactorSub) ConnDec() {
	r.connCount.Dec()
}

func (r *ReactorSub) run() {
	defer func() {
		if err := recover(); err != nil {
			r.Panic("reactorSub panic", zap.Any("err", err), zap.Stack("stack"))
		}
	}()

	err := r.poller.PollingWithSnapshot(r.registration.Load, r.handleEvent)

	if err != nil && !r.stopped.Load() {
		r.Panic("poller error", zap.Error(err), zap.Int("idx", r.idx))
	}
}

func (r *ReactorSub) handleEvent(fd int, generation uint64, event netpoll.PollEvent) error {
	conn := r.eg.getConnAt(fd, generation)
	if conn == nil || conn.IsClosed() {
		return nil
	}
	switch event {
	case netpoll.PollEventClose:
		r.Debug("conn 连接关闭！", zap.Int64("id", conn.ID()), zap.Int("fd", fd))
		return r.CloseConn(conn, unix.ECONNRESET)
	case netpoll.PollEventRead:
		return r.read(conn)
	case netpoll.PollEventWrite:
		return r.write(conn)
	}
	return nil
}

func (d *DefaultConn) readFD(buf []byte) (int, error) {
	// Unix 已接受的 socket 是非阻塞的，读系统调用必须与关闭/复用串行。
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed.Load() {
		return 0, net.ErrClosed
	}
	return d.fd.Read(buf)
}

func (r *ReactorSub) CloseConn(c Conn, er error) (rerr error) {
	r.Debug("close connn", zap.Error(er), zap.Int64("id", c.ID()), zap.Int("fd", c.Fd().fd))
	return c.CloseWithErr(er)
}

func (r *ReactorSub) read(c Conn) error {
	var err error
	var n int
	if n, err = c.ReadToInboundBuffer(); err != nil {
		// A TLS read can return valid plaintext followed by a fatal record
		// error from the same socket read. Deliver the plaintext before
		// closing the connection for the fatal error.
		if _, hasNewData := err.(*readErrorWithData); hasNewData && !c.IsClosed() {
			if dataErr := r.eg.eventHandler.OnData(c); dataErr != nil && dataErr != unix.EAGAIN {
				if err1 := r.CloseConn(c, dataErr); err1 != nil {
					r.Warn("failed to close conn", zap.Error(err1))
				}
				r.Warn("failed to call OnData", zap.Error(dataErr))
				return nil
			}
		}
		if err == unix.EAGAIN {
			return nil
		}
		if err1 := r.CloseConn(c, err); err1 != nil {
			r.Warn("failed to close conn", zap.Error(err1))
		}
		return nil
	}
	if n == 0 {
		return r.CloseConn(c, os.NewSyscallError("read", unix.ECONNRESET))
	}
	if c.IsClosed() {
		return nil
	}
	if err = r.eg.eventHandler.OnData(c); err != nil {
		if err == unix.EAGAIN {
			return nil
		}
		if err1 := r.CloseConn(c, err); err1 != nil {
			r.Warn("failed to close conn", zap.Error(err1))
		}
		r.Warn("failed to call OnData", zap.Error(err))
		return nil
	}
	return nil
}

func (r *ReactorSub) write(c Conn) error {
	err := c.Flush()
	switch err {
	case nil:
	case unix.EAGAIN:
		return nil
	default:
		return r.CloseConn(c, os.NewSyscallError("write", err))
	}
	return nil
}
