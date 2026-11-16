package wknet

import (
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/RussellLuo/timingwheel"
	"github.com/WuKongIM/WuKongIM/pkg/ring"
	"github.com/WuKongIM/crypto/tls"
	"github.com/sasha-s/go-deadlock"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

type ConnStats struct {
	InMsgs         atomic.Int64 // 收到客户端消息数量
	OutMsgs        atomic.Int64 // 下发消息数量
	InMsgBytes     atomic.Int64 // 收到消息字节数
	OutMsgBytes    atomic.Int64 // 下发消息字节数
	InPackets      atomic.Int64 // 收到包数量
	OutPackets     atomic.Int64 // 下发包数量
	InPacketBytes  atomic.Int64 // 收到包字节数
	OutPacketBytes atomic.Int64 // 下发包字节数
}

func NewConnStats() *ConnStats {

	return &ConnStats{}
}

type Conn interface {
	// ID returns the connection id.
	ID() int64
	// SetID sets the connection id.
	SetID(id int64)
	// UID returns the user uid.
	UID() string
	// SetUID sets the user uid.
	SetUID(uid string)
	// DeviceLevel() uint8
	// SetDeviceLevel(deviceLevel uint8)
	// DeviceFlag returns the device flag.
	// DeviceFlag() uint8
	// SetDeviceFlag sets the device flag.
	// SetDeviceFlag(deviceFlag uint8)
	// DeviceID returns the device id.
	// DeviceID() string
	// SetValue sets the value associated with key to value.
	SetValue(key string, value interface{})
	// Value returns the value associated with key.
	Value(key string) interface{}
	// SetDeviceID sets the device id.
	// SetDeviceID(deviceID string)
	// Flush flushes the data to the connection.
	Flush() error
	// Read reads the data from the connection.
	Read(buf []byte) (int, error)
	// Peek peeks the data from the connection.
	Peek(n int) ([]byte, error)
	// Discard discards the data from the connection.
	Discard(n int) (int, error)
	// Write writes the data to the connection. TODO: Locking is required when calling write externally
	Write(b []byte) (int, error)
	// WriteToOutboundBuffer writes the data to the outbound buffer.  Thread safety
	WriteToOutboundBuffer(b []byte) (int, error)
	// Wake wakes up the connection write.
	WakeWrite() error
	// Fd returns the file descriptor of the connection.
	Fd() NetFd
	// IsClosed returns true if the connection is closed.
	IsClosed() bool
	// Close closes the connection.
	Close() error
	CloseWithErr(err error) error
	// RemoteAddr returns the remote network address.
	RemoteAddr() net.Addr
	// SetRemoteAddr sets the remote network address.
	SetRemoteAddr(addr net.Addr)
	// LocalAddr returns the local network address.
	LocalAddr() net.Addr
	// ReactorSub returns the reactor sub.
	ReactorSub() *ReactorSub
	// ReadToInboundBuffer read data from connection and  write to inbound buffer
	ReadToInboundBuffer() (int, error)
	SetContext(ctx interface{})
	Context() interface{}
	// IsAuthed returns true if the connection is authed.
	IsAuthed() bool
	// SetAuthed sets the connection is authed.
	SetAuthed(authed bool)
	// LastActivity returns the last activity time.
	LastActivity() time.Time
	// Uptime returns the connection uptime.
	Uptime() time.Time
	// SetMaxIdle sets the connection max idle time.
	// If the connection is idle for more than the specified duration, it will be closed.
	SetMaxIdle(duration time.Duration)

	InboundBuffer() InboundBuffer
	OutboundBuffer() OutboundBuffer

	SetDeadline(t time.Time) error
	SetReadDeadline(t time.Time) error
	SetWriteDeadline(t time.Time) error

	// ConnStats returns the connection stats.
	// ConnStats() *ConnStats
}

type IWSConn interface {
	WriteServerBinary(data []byte) error
	WriteServerText(data []byte) error
}

type DefaultConn struct {
	fd             NetFd
	remoteAddr     net.Addr
	localAddr      net.Addr
	eg             *Engine
	reactorSub     *ReactorSub
	inboundBuffer  InboundBuffer  // inboundBuffer InboundBuffer
	outboundBuffer OutboundBuffer // outboundBuffer OutboundBuffer
	closed         atomic.Bool    // if the connection is closed
	isWAdded       bool           // if the connection is added to the write event
	mu             deadlock.RWMutex
	context        atomic.Value
	authed         atomic.Bool // if the connection is authed
	id             atomic.Int64
	uid            atomic.String
	valueMap       sync.Map

	// mu 保护发送、底层 FD 关闭、连接身份及空闲定时器。
	owner          Conn
	idleGeneration uint64
	idleTimer      *timingwheel.Timer

	uptime       atomic.Time
	lastActivity atomic.Time

	// 接收解析可以写回 TLS/WS 数据，所以接收和发送使用不同的锁。
	recvMu           sync.Mutex
	temporaryInbound []InboundBuffer

	wklog.Log
}

func (d *DefaultConn) defaultConn() *DefaultConn { return d }

func defaultConnOf(c Conn) *DefaultConn {
	if wrapper, ok := c.(interface{ defaultConn() *DefaultConn }); ok {
		return wrapper.defaultConn()
	}
	return nil
}

func bindConnOwner(conn Conn) {
	if d := defaultConnOf(conn); d != nil {
		d.mu.Lock()
		defer d.mu.Unlock()
		if !d.closed.Load() {
			d.owner = conn
		}
	}
}

func GetDefaultConn(id int64, connFd NetFd, localAddr, remoteAddr net.Addr, eg *Engine, reactorSub *ReactorSub) *DefaultConn {
	// DefaultConn participates in async close/read/idle-timeout paths.
	// Reusing it through sync.Pool allows stale goroutines to act on a
	// different accepted connection after release.
	defaultConn := &DefaultConn{}
	defaultConn.id.Store(id)
	defaultConn.fd = connFd
	defaultConn.remoteAddr = remoteAddr
	defaultConn.localAddr = localAddr
	defaultConn.isWAdded = false
	defaultConn.authed.Store(false)
	defaultConn.closed.Store(false)
	defaultConn.uid.Store("")
	defaultConn.eg = eg
	defaultConn.reactorSub = reactorSub
	defaultConn.valueMap = sync.Map{}
	defaultConn.context = atomic.Value{}
	defaultConn.lastActivity.Store(time.Now())
	defaultConn.uptime.Store(time.Now())
	defaultConn.Log = wklog.NewWKLog(fmt.Sprintf("Conn[[reactor-%d]%d]", reactorSub.idx, id))

	defaultConn.inboundBuffer = eg.eventHandler.OnNewInboundConn(defaultConn, eg)
	defaultConn.outboundBuffer = eg.eventHandler.OnNewOutboundConn(defaultConn, eg)

	return defaultConn
}

func CreateConn(id int64, connFd NetFd, localAddr, remoteAddr net.Addr, eg *Engine, reactorSub *ReactorSub) (Conn, error) {

	// defaultConn := &DefaultConn{
	// 	id:         id,
	// 	fd:         connFd,
	// 	remoteAddr: remoteAddr,
	// 	localAddr:  localAddr,
	// 	eg:         eg,
	// 	reactorSub: reactorSub,
	// 	closed:     false,
	// 	valueMap:   map[string]interface{}{},
	// 	uptime:     time.Now(),
	// 	Log:        wklog.NewWKLog(fmt.Sprintf("Conn[%d]", id)),
	// }

	defaultConn := GetDefaultConn(id, connFd, localAddr, remoteAddr, eg, reactorSub)
	if eg.options.TCPTLSConfig != nil {
		tc := newTLSConn(defaultConn)
		tlsCn := tls.Server(tc, eg.options.TCPTLSConfig)
		tc.tlsconn = tlsCn
		return tc, nil
	}
	return defaultConn, nil
}

func (d *DefaultConn) ID() int64 {
	return d.id.Load()
}

func (d *DefaultConn) SetID(id int64) {
	d.id.Store(id)
}

func (d *DefaultConn) ReadToInboundBuffer() (int, error) {
	var n int
	d.recvMu.Lock()
	defer func() {
		d.recvMu.Unlock()
		d.reportRead(n)
	}()
	readBuffer := d.reactorSub.ReadBuffer
	var err error
	n, err = d.readFD(readBuffer)
	if err != nil || n == 0 {
		return 0, err
	}
	if d.overflowForInbound(n) {
		return 0, fmt.Errorf("inbound buffer overflow, fd: %d buffSize:%d n: %d currentSize: %d maxSize: %d", d.fd, d.inboundBuffer.BoundBufferSize(), n, d.inboundBuffer.BoundBufferSize()+n, d.eg.options.MaxReadBufferSize)
	}
	d.KeepLastActivity()
	_, err = d.inboundBuffer.Write(readBuffer[:n])
	return n, err
}

func (d *DefaultConn) reportRead(n int) {
	if n > 0 && d.eg.options.Event.OnReadBytes != nil {
		d.eg.options.Event.OnReadBytes(n)
	}
}

func (d *DefaultConn) KeepLastActivity() {
	d.lastActivity.Store(time.Now())
}

func (d *DefaultConn) Read(buf []byte) (int, error) {
	d.recvMu.Lock()
	defer d.recvMu.Unlock()
	if d.inboundBuffer.IsEmpty() {
		return 0, nil
	}
	n, err := d.inboundBuffer.Read(buf)
	if n == len(buf) {
		return n, nil
	}
	return n, err
}

func (d *DefaultConn) Write(b []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed.Load() {
		return -1, net.ErrClosed
	}
	n, err := d.write(b)
	if err != nil {
		return 0, err
	}
	return n, nil
}

// write to outbound buffer
func (d *DefaultConn) WriteToOutboundBuffer(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed.Load() {
		return -1, net.ErrClosed
	}
	return d.outboundBuffer.Write(b)

}

func (d *DefaultConn) WakeWrite() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed.Load() {
		return net.ErrClosed
	}
	return d.addWriteIfNotExist()
}

func (d *DefaultConn) IsClosed() bool {

	return d.closed.Load()
}

func (d *DefaultConn) Flush() error {
	if d.closed.Load() {
		return net.ErrClosed
	}
	return d.flush()
}
func (d *DefaultConn) Fd() NetFd {

	return d.fd
}

func (d *DefaultConn) close(_ error) error {
	d.mu.Lock()
	owner := d.closeLocked()
	d.mu.Unlock()
	d.finishClose(owner)
	return nil
}

// closeLocked 在释放 FD 前原子地认领关闭；上层回调必须在解锁后调用。
func (d *DefaultConn) closeLocked() Conn {
	if d.closed.Load() {
		return nil
	}
	d.closed.Store(true)
	d.cancelIdleLocked()

	owner := d.owner
	if owner == nil {
		owner = d
	}

	// 正常关闭和连接重置都要先注销：若底层 socket 还有其他引用，仅关闭当前 FD 不会移除 epoll 事件。
	if err := d.reactorSub.DeleteFd(d); err != nil {
		d.Debug("delete fd from poller error", zap.Error(err), zap.Int("fd", d.Fd().fd), zap.String("uid", d.uid.Load()))
	}

	// 先删旧连接索引，再释放 FD，避免误删已经复用这个 FD 的新连接。
	if d.eg.removeConn(owner) {
		d.reactorSub.ConnDec()
	}
	_ = d.fd.Close()
	if err := d.outboundBuffer.Release(); err != nil {
		d.Debug("outboundBuffer release error", zap.Error(err))
	}
	return owner
}

func (d *DefaultConn) finishClose(owner Conn) {
	if owner == nil {
		return
	}
	d.eg.eventHandler.OnClose(owner)
	d.recvMu.Lock()
	defer d.recvMu.Unlock()
	if err := d.inboundBuffer.Release(); err != nil {
		d.Debug("inboundBuffer release error", zap.Error(err))
	}
	for _, buffer := range d.temporaryInbound {
		if err := buffer.Release(); err != nil {
			d.Debug("temporary inbound buffer release error", zap.Error(err))
		}
	}
}

func (d *DefaultConn) Close() error {
	return d.close(nil)
}

func (d *DefaultConn) CloseWithErr(err error) error {

	return d.close(err)
}

func (d *DefaultConn) RemoteAddr() net.Addr {

	return d.remoteAddr
}

func (d *DefaultConn) SetRemoteAddr(addr net.Addr) {
	d.remoteAddr = addr
}

func (d *DefaultConn) LocalAddr() net.Addr {
	return d.localAddr
}

func (d *DefaultConn) SetDeadline(t time.Time) error {
	if err := d.SetReadDeadline(t); err != nil {
		return err
	}
	return d.SetWriteDeadline(t)
}

func (d *DefaultConn) SetReadDeadline(t time.Time) error {
	return ErrUnsupportedOp
}

func (d *DefaultConn) SetWriteDeadline(t time.Time) error {
	return ErrUnsupportedOp
}

func (d *DefaultConn) Peek(n int) ([]byte, error) {
	d.recvMu.Lock()
	defer d.recvMu.Unlock()
	totalLen := d.inboundBuffer.BoundBufferSize()
	if n > totalLen {
		return nil, io.ErrShortBuffer
	} else if n <= 0 {
		n = totalLen
	}
	if d.inboundBuffer.IsEmpty() {
		return nil, nil
	}
	head, tail := d.inboundBuffer.Peek(n)
	d.reactorSub.cache.Reset()
	d.reactorSub.cache.Write(head)
	d.reactorSub.cache.Write(tail)

	data := d.reactorSub.cache.Bytes()

	resultData := make([]byte, len(data))
	copy(resultData, data) // TODO: 这里需要复制一份，否则多线程下解析数据包会有问题 本人测试 15个连接15个消息 在协程下打印sendPacket的payload会有数据错误问题

	return resultData, nil
}

func (d *DefaultConn) Discard(n int) (int, error) {
	d.recvMu.Lock()
	defer d.recvMu.Unlock()
	return d.inboundBuffer.Discard(n)
}

func (d *DefaultConn) ReactorSub() *ReactorSub {
	return d.reactorSub
}

func (d *DefaultConn) SetContext(ctx interface{}) {
	d.context.Store(ctx)
}
func (d *DefaultConn) Context() interface{} {
	return d.context.Load()
}

func (d *DefaultConn) IsAuthed() bool {
	return d.authed.Load()
}
func (d *DefaultConn) SetAuthed(authed bool) {
	d.authed.Store(authed)
}

func (d *DefaultConn) UID() string {
	return d.uid.Load()
}
func (d *DefaultConn) SetUID(uid string) {
	d.uid.Store(uid)
}

func (d *DefaultConn) SetValue(key string, value interface{}) {
	d.valueMap.Store(key, value)
}
func (d *DefaultConn) Value(key string) interface{} {

	value, _ := d.valueMap.Load(key)
	return value
}

func (d *DefaultConn) InboundBuffer() InboundBuffer {
	return d.inboundBuffer
}

func (d *DefaultConn) OutboundBuffer() OutboundBuffer {
	return d.outboundBuffer
}

func (d *DefaultConn) LastActivity() time.Time {
	return d.lastActivity.Load()
}

func (d *DefaultConn) Uptime() time.Time {
	return d.uptime.Load()
}

func (d *DefaultConn) SetMaxIdle(maxIdle time.Duration) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed.Load() {
		d.Debug("connection is closed, setMaxIdle failed")
		return
	}

	d.cancelIdleLocked()
	if maxIdle > 0 {
		d.scheduleIdleLocked(d.idleGeneration, maxIdle)
	}
}

func (d *DefaultConn) scheduleIdleLocked(generation uint64, maxIdle time.Duration) {
	// 由当前代次检查之后才续排，已经开始执行的旧回调不会留下一条重复定时任务。
	d.idleTimer = d.eg.timingWheel.AfterFunc(maxIdle/2, func() {
		d.expireIdle(generation, maxIdle)
	})
}

func (d *DefaultConn) cancelIdleLocked() {
	d.idleGeneration++
	if d.idleTimer != nil {
		d.idleTimer.Stop()
		d.idleTimer = nil
	}
}

func (d *DefaultConn) expireIdle(generation uint64, maxIdle time.Duration) {
	d.mu.Lock()
	// 用同一把锁检查并认领关闭，旧回调不能越过更新后的空闲期限。
	if generation != d.idleGeneration || d.closed.Load() {
		d.mu.Unlock()
		return
	}
	if d.lastActivity.Load().Add(maxIdle).After(time.Now()) {
		d.scheduleIdleLocked(generation, maxIdle)
		d.mu.Unlock()
		return
	}
	d.Debug("max idle time exceeded, close the connection", zap.Duration("maxIdle", maxIdle), zap.Duration("lastActivity", time.Since(d.lastActivity.Load())), zap.String("conn", d.String()))
	owner := d.closeLocked()
	d.mu.Unlock()
	d.finishClose(owner)
}

func (d *DefaultConn) flush() error {

	d.mu.Lock()
	// defer  d.mu.Unlock() // 这里不能defer锁，因为d.reactorSub.CloseConn里也会调用close的锁，导致死锁

	if d.closed.Load() {
		d.mu.Unlock()
		return net.ErrClosed
	}

	if d.outboundBuffer.IsEmpty() {
		_ = d.removeWriteIfExist()
		d.mu.Unlock()
		return nil
	}
	var (
		n   int
		err error
	)

	head, tail := d.outboundBuffer.Peek(-1)
	n, err = d.writeDirect(head, tail)
	_, _ = d.outboundBuffer.Discard(n)
	if d.eg.options.Event.OnWirteBytes != nil {
		d.eg.options.Event.OnWirteBytes(n)
	}
	d.mu.Unlock()

	switch err {
	case nil:
	case syscall.EAGAIN:
		d.Error("write error", zap.Error(err))
	default:
		// d.reactorSub.CloseConn 里使用了d.mu的锁
		err = d.reactorSub.CloseConn(d, os.NewSyscallError("write", err))
		if err != nil {
			d.Error("failed to close conn", zap.Error(err))
			return err
		}
	}
	// All data have been drained, it's no need to monitor the writable events,
	// remove the writable event from poller to help the future event-loops.

	d.mu.Lock()
	if d.outboundBuffer.IsEmpty() {
		_ = d.removeWriteIfExist()
	}
	d.mu.Unlock()
	return nil

}

func (d *DefaultConn) WriteDirect(head, tail []byte) (int, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.writeDirect(head, tail)
}

func (d *DefaultConn) writeDirect(head, tail []byte) (int, error) {
	if d.closed.Load() {
		return -1, net.ErrClosed
	}
	var (
		n   int
		err error
	)
	if len(head) > 0 && len(tail) > 0 {
		n, err = d.fd.Write(append(head, tail...))
	} else {
		if len(head) > 0 {
			n, err = d.fd.Write(head)
		} else if len(tail) > 0 {
			n, err = d.fd.Write(tail)
		}
	}
	return n, err
}

func (d *DefaultConn) write(b []byte) (int, error) {
	if d.closed.Load() {
		return -1, net.ErrClosed
	}
	n := len(b)
	if n == 0 {
		return 0, nil
	}
	if d.overflowForOutbound(len(b)) { // overflow check
		return 0, syscall.EINVAL
	}
	var err error
	n, err = d.outboundBuffer.Write(b)
	if err != nil {
		return 0, err
	}
	if err = d.addWriteIfNotExist(); err != nil {
		return n, err
	}
	return n, nil
}

func (d *DefaultConn) addWriteIfNotExist() error {
	if d.closed.Load() {
		return net.ErrClosed
	}
	return d.reactorSub.AddWrite(d)
}

func (d *DefaultConn) removeWriteIfExist() error {
	// if d.isWAdded {
	// 	d.isWAdded = false
	// 	return d.reactorSub.RemoveWrite(d)
	// }
	if d.closed.Load() {
		return net.ErrClosed
	}
	return d.reactorSub.RemoveWrite(d)
}

func (d *DefaultConn) overflowForOutbound(n int) bool {
	maxWriteBufferSize := d.eg.options.MaxWriteBufferSize
	return maxWriteBufferSize > 0 && (d.outboundBuffer.BoundBufferSize()+n > maxWriteBufferSize)
}
func (d *DefaultConn) overflowForInbound(n int) bool {
	maxReadBufferSize := d.eg.options.MaxReadBufferSize
	return maxReadBufferSize > 0 && (d.inboundBuffer.BoundBufferSize()+n > maxReadBufferSize)
}

func (d *DefaultConn) String() string {

	return fmt.Sprintf("Conn[%d] fd=%d", d.id.Load(), d.fd)
}

type TLSConn struct {
	d                *DefaultConn
	tlsconn          *tls.Conn
	tmpInboundBuffer InboundBuffer // inboundBuffer InboundBuffer
}

type readErrorWithData struct {
	err error
}

func (e *readErrorWithData) Error() string {
	return e.err.Error()
}

func (e *readErrorWithData) Unwrap() error {
	return e.err
}

func newTLSConn(d *DefaultConn) *TLSConn {
	buffer := d.eg.eventHandler.OnNewInboundConn(d, d.eg)
	d.temporaryInbound = append(d.temporaryInbound, buffer)
	return &TLSConn{d: d, tmpInboundBuffer: buffer}
}

func (t *TLSConn) defaultConn() *DefaultConn { return t.d }

func (t *TLSConn) ReadToInboundBuffer() (int, error) {
	var n int
	t.d.recvMu.Lock()
	defer func() {
		t.d.recvMu.Unlock()
		t.d.reportRead(n)
	}()
	readBuffer := t.d.reactorSub.ReadBuffer
	var err error
	n, err = t.d.readFD(readBuffer)
	if err != nil || n == 0 {
		return 0, err
	}
	_, err = t.tmpInboundBuffer.Write(readBuffer[:n]) // 将tls加密的内容写到tmpInboundBuffer内， tls会从tmpInboundBuffer读取数据（BuffReader接口）
	if err != nil {
		return 0, err
	}
	t.d.KeepLastActivity()

	decodedN := 0
	for {
		tlsN, readErr := t.tlsconn.Read(readBuffer) // 这里其实是把tmpInboundBuffer的数据解密后放到readBuffer内了
		decodedN += tlsN
		_, err = t.d.inboundBuffer.Write(readBuffer[:tlsN]) // 再将readBuffer的数据放到inboundBuffer内，然后供上层应用读取
		if err != nil {
			return n, err
		}
		if readErr != nil {
			if readErr == tls.ErrDataNotEnough {
				return n, nil
			}
			if decodedN > 0 {
				return n, &readErrorWithData{err: readErr}
			}
			return n, readErr
		}
		if tlsN == 0 {
			break
		}
	}
	return n, err
}
func (t *TLSConn) BuffReader(needs int) io.Reader {
	return &eofBuff{
		buff:  t.tmpInboundBuffer,
		needs: needs,
	}
}

func (t *TLSConn) BuffWriter() io.Writer {
	return t.d
}

func (t *TLSConn) ID() int64 {
	return t.d.ID()
}
func (t *TLSConn) SetID(id int64) {
	t.d.SetID(id)
}

func (t *TLSConn) UID() string {
	return t.d.UID()
}

func (t *TLSConn) SetUID(uid string) {
	t.d.SetUID(uid)
}

func (t *TLSConn) Fd() NetFd {
	return t.d.Fd()
}

func (t *TLSConn) LocalAddr() net.Addr {
	return t.d.LocalAddr()
}

func (t *TLSConn) RemoteAddr() net.Addr {
	return t.d.RemoteAddr()
}

func (t *TLSConn) SetRemoteAddr(addr net.Addr) {
	t.d.SetRemoteAddr(addr)
}

func (t *TLSConn) Read(b []byte) (int, error) {
	t.d.recvMu.Lock()
	defer t.d.recvMu.Unlock()
	return t.tlsconn.Read(b)
}

func (t *TLSConn) Write(b []byte) (int, error) {
	return t.tlsconn.Write(b)
}

func (t *TLSConn) SetDeadline(tim time.Time) error {
	return t.d.SetDeadline(tim)
}

func (t *TLSConn) SetReadDeadline(tim time.Time) error {
	return t.d.SetReadDeadline(tim)
}

func (t *TLSConn) SetWriteDeadline(tim time.Time) error {
	return t.d.SetWriteDeadline(tim)
}

func (t *TLSConn) Close() error {
	return t.d.Close()
}

func (t *TLSConn) CloseWithErr(err error) error {
	return t.d.CloseWithErr(err)
}

func (t *TLSConn) Context() interface{} {
	return t.d.Context()
}

func (t *TLSConn) SetContext(ctx interface{}) {
	t.d.SetContext(ctx)
}

func (t *TLSConn) WakeWrite() error {
	return t.d.WakeWrite()
}

func (t *TLSConn) Discard(n int) (int, error) {
	return t.d.Discard(n)
}

func (t *TLSConn) InboundBuffer() InboundBuffer {
	return t.d.InboundBuffer()
}

func (t *TLSConn) OutboundBuffer() OutboundBuffer {
	return t.d.OutboundBuffer()
}

func (t *TLSConn) IsAuthed() bool {
	return t.d.IsAuthed()
}

func (t *TLSConn) SetAuthed(authed bool) {
	t.d.SetAuthed(authed)
}

func (t *TLSConn) IsClosed() bool {
	return t.d.IsClosed()
}

func (t *TLSConn) LastActivity() time.Time {
	return t.d.LastActivity()
}

func (t *TLSConn) Peek(n int) ([]byte, error) {
	return t.d.Peek(n)
}

func (t *TLSConn) ReactorSub() *ReactorSub {
	return t.d.ReactorSub()
}

func (t *TLSConn) Flush() error {
	return t.d.Flush()
}

func (t *TLSConn) SetValue(key string, value interface{}) {
	t.d.SetValue(key, value)
}

func (t *TLSConn) Value(key string) interface{} {
	return t.d.Value(key)
}

func (t *TLSConn) Uptime() time.Time {
	return t.d.Uptime()
}

func (t *TLSConn) WriteToOutboundBuffer(b []byte) (int, error) {
	return t.d.WriteToOutboundBuffer(b)
}

func (t *TLSConn) SetMaxIdle(maxIdle time.Duration) {
	t.d.SetMaxIdle(maxIdle)
}

func (t *TLSConn) String() string {
	return t.d.String()
}

type eofBuff struct {
	buff  InboundBuffer
	needs int
}

func (e *eofBuff) Read(p []byte) (int, error) {
	n, err := e.buff.Read(p)
	e.needs -= n

	if e.needs > 0 && err == ring.ErrIsEmpty {
		return n, tls.ErrDataNotEnough
	}
	if e.needs <= 0 && err == nil {
		return n, io.EOF
	}
	if err != nil {
		if err == ring.ErrIsEmpty {
			return n, io.EOF
		}
		return n, err
	}
	return n, err
}

// func getConnFd(conn net.Conn) (int, error) {
// 	sc, ok := conn.(interface {
// 		SyscallConn() (syscall.RawConn, error)
// 	})
// 	if !ok {
// 		return 0, errors.New("RawConn Unsupported")
// 	}
// 	rc, err := sc.SyscallConn()
// 	if err != nil {
// 		return 0, errors.New("RawConn Unsupported")
// 	}
// 	var newFd int
// 	errCtrl := rc.Control(func(fd uintptr) {
// 		newFd, err = syscall.Dup(int(fd))
// 	})
// 	if errCtrl != nil {
// 		return 0, errCtrl
// 	}
// 	if err != nil {
// 		return 0, err
// 	}

// 	return newFd, nil
// }

type connMatrix struct {
	connCount atomic.Int32
	conns     map[int]connRegistration
}

type connRegistration struct {
	conn       Conn
	generation uint64
}

func newConnMatrix() *connMatrix {
	return &connMatrix{
		conns: make(map[int]connRegistration),
	}
}

func (cm *connMatrix) iterate(f func(Conn) bool) {
	for _, reg := range cm.conns {
		if reg.conn != nil {
			if !f(reg.conn) {
				return
			}
		}
	}
}
func (cm *connMatrix) countAdd(delta int32) {
	cm.connCount.Add(delta)
}

func (cm *connMatrix) addConn(c Conn, generation uint64) {
	fd := c.Fd().Fd()
	if _, ok := cm.conns[fd]; !ok {
		cm.countAdd(1)
	}
	cm.conns[fd] = connRegistration{conn: c, generation: generation}
}

func (cm *connMatrix) delConn(c Conn) bool {
	fd := c.Fd().Fd()
	if cm.conns[fd].conn != c {
		return false
	}
	delete(cm.conns, fd)
	cm.countAdd(-1)
	return true
}

func (cm *connMatrix) getConn(fd int) Conn {
	return cm.conns[fd].conn
}
func (cm *connMatrix) loadCount() (n int32) {
	return cm.connCount.Load()
}
