package wknet

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/WuKongIM/crypto/tls"
	"go.uber.org/zap"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

func CreateWSConn(id int64, connFd NetFd, localAddr, remoteAddr net.Addr, eg *Engine, reactorSub *ReactorSub) (Conn, error) {
	defaultConn := GetDefaultConn(id, connFd, localAddr, remoteAddr, eg, reactorSub)
	return NewWSConn(defaultConn), nil
}

func CreateWSSConn(id int64, connFd NetFd, localAddr, remoteAddr net.Addr, eg *Engine, reactorSub *ReactorSub) (Conn, error) {
	defaultConn := GetDefaultConn(id, connFd, localAddr, remoteAddr, eg, reactorSub)
	tc := newTLSConn(defaultConn)
	tlsCn := tls.Server(tc, eg.options.WSTLSConfig)
	tc.tlsconn = tlsCn
	return NewWSSConn(tc), nil
}

type WSConn struct {
	*DefaultConn
	stream           wsStream
	upgraded         bool
	tmpInboundBuffer InboundBuffer // inboundBuffer InboundBuffer
}

func NewWSConn(d *DefaultConn) *WSConn {
	buffer := d.eg.eventHandler.OnNewInboundConn(d, d.eg)
	d.temporaryInbound = append(d.temporaryInbound, buffer)
	return &WSConn{DefaultConn: d, tmpInboundBuffer: buffer, stream: wsStream{limit: d.eg.options.MaxReadBufferSize}}
}

func (w *WSConn) ReadToInboundBuffer() (int, error) {
	var n int
	w.recvMu.Lock()
	defer func() {
		w.recvMu.Unlock()
		w.reportRead(n)
	}()
	readBuffer := w.reactorSub.ReadBuffer
	var err error
	n, err = w.readFD(readBuffer)
	if err != nil || n == 0 {
		return 0, err
	}
	_, err = w.tmpInboundBuffer.Write(readBuffer[:n])
	if err != nil {
		return 0, err
	}
	w.KeepLastActivity()

	err = w.unpacketWSData()

	return n, err
}

func (w *WSConn) WriteServerBinary(data []byte) error { return w.writeFrame(ws.OpBinary, data) }

func (w *WSConn) WriteServerText(data []byte) error { return w.writeFrame(ws.OpText, data) }

func (w *WSConn) writeFrame(op ws.OpCode, data []byte) error {
	w.stream.sendMu.Lock()
	defer w.stream.sendMu.Unlock()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stream.closeSent || w.closed.Load() {
		return net.ErrClosed
	}
	return wsutil.WriteServerMessage(w.outboundBuffer, op, data)
}

func (w *WSConn) CloseWithErr(err error) error { return closeWSWithErr(w.DefaultConn, err) }

// 解包ws的数据
func (w *WSConn) unpacketWSData() error {
	if !w.upgraded {
		return w.upgrade()
	}
	messages, err := w.decode()
	return w.stream.deliver(w.DefaultConn, w.inboundBuffer, messages, err)
}

func (w *WSConn) decode() ([]wsutil.Message, error) {
	buff, err := w.PeekFromTemp(-1)
	if err != nil {
		return nil, err
	}
	return w.stream.decode(buff, w.DiscardFromTemp)
}

func (w *WSConn) upgrade() error {
	buff, err := w.PeekFromTemp(-1)
	if err != nil {
		return err
	}
	tmpReader := bytes.NewReader(buff)
	tmpWriter := bytes.NewBuffer(nil)
	_, err = ws.Upgrade(&readWrite{
		Reader: tmpReader,
		Writer: tmpWriter,
	})
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF { //数据不完整
			return nil
		}
		w.DiscardFromTemp(len(buff)) // 发送错误，丢弃数据
		return err
	}

	// 解析http请求
	req, err := w.parseHttpRequest(buff)
	if err != nil {
		return err
	}

	realIp := w.getRealIp(req) // 获取真实ip
	realPortStr := req.Header.Get("X-Real-Port")
	if strings.TrimSpace(realIp) != "" {
		realPort := 0
		if strings.TrimSpace(realPortStr) != "" {
			realPort = wkutil.ParseInt(realPortStr)
		} else {
			if w.remoteAddr != nil {
				realPort = w.remoteAddr.(*net.TCPAddr).Port
			}
		}
		w.SetRemoteAddr(&net.TCPAddr{
			IP:   net.ParseIP(realIp),
			Port: realPort,
		})
	}

	_, err = w.Write(tmpWriter.Bytes())
	if err != nil {
		return err
	}

	w.DiscardFromTemp(len(buff) - tmpReader.Len())
	w.upgraded = true
	return nil
}

func (w *WSConn) getRealIp(r *http.Request) string {
	realIp := r.Header.Get("X-Forwarded-For")
	if strings.TrimSpace(realIp) == "" {
		realIp = r.Header.Get("X-Real-IP")
	}
	return realIp
}

func (w *WSConn) parseHttpRequest(data []byte) (*http.Request, error) {
	requestStr := string(data)

	// 创建一个虚拟的Request对象
	req, err := http.ReadRequest(bufio.NewReader(strings.NewReader(requestStr)))
	if err != nil {
		fmt.Println("Error parsing request:", err)
		w.Error("Error parsing request", zap.Error(err))
		return nil, err
	}
	return req, nil
}

func (w *WSConn) PeekFromTemp(n int) ([]byte, error) {
	totalLen := w.tmpInboundBuffer.BoundBufferSize()
	if n > totalLen {
		return nil, io.ErrShortBuffer
	} else if n <= 0 {
		n = totalLen
	}
	if w.tmpInboundBuffer.IsEmpty() {
		return nil, nil
	}
	head, tail := w.tmpInboundBuffer.Peek(n)
	w.reactorSub.cache.Reset()
	w.reactorSub.cache.Write(head)
	w.reactorSub.cache.Write(tail)

	data := w.reactorSub.cache.Bytes()
	return data, nil
}

func (w *WSConn) DiscardFromTemp(n int) {
	_, _ = w.tmpInboundBuffer.Discard(n)
}

type readWrite struct {
	io.Reader
	io.Writer
}

type WSSConn struct {
	*TLSConn
	stream   wsStream
	upgraded bool

	wsTmpInboundBuffer InboundBuffer // inboundBuffer InboundBuffer
}

func NewWSSConn(tlsConn *TLSConn) *WSSConn {
	d := tlsConn.d
	buffer := d.eg.eventHandler.OnNewInboundConn(d, d.eg)
	d.temporaryInbound = append(d.temporaryInbound, buffer)
	return &WSSConn{TLSConn: tlsConn, wsTmpInboundBuffer: buffer, stream: wsStream{limit: d.eg.options.MaxReadBufferSize}}
}

func (w *WSSConn) ReadToInboundBuffer() (int, error) {
	var n int
	w.d.recvMu.Lock()
	defer func() {
		w.d.recvMu.Unlock()
		w.d.reportRead(n)
	}()
	readBuffer := w.d.reactorSub.ReadBuffer
	var err error
	n, err = w.d.readFD(readBuffer)
	if err != nil || n == 0 {
		return 0, err
	}

	_, err = w.tmpInboundBuffer.Write(readBuffer[:n])
	if err != nil {
		return 0, err
	}

	var terminal error
	for {
		tlsN, readErr := w.tlsconn.Read(readBuffer)
		if tlsN > 0 {
			if _, err := w.wsTmpInboundBuffer.Write(readBuffer[:tlsN]); err != nil {
				return n, err
			}
		}
		if readErr != nil || tlsN == 0 {
			if readErr != tls.ErrDataNotEnough {
				terminal = readErr
			}
			break
		}
	}

	w.d.KeepLastActivity()
	before := w.d.inboundBuffer.BoundBufferSize()
	if err := w.unpacketWSData(); err != nil {
		return n, err
	}
	// 同次读取的 TLS 尾部可能不完整或出错，前面已经解出的完整消息仍须交付。
	if terminal != nil && w.d.inboundBuffer.BoundBufferSize() > before {
		return n, &readErrorWithData{err: terminal}
	}
	return n, terminal
}

func (w *WSSConn) peekFromWSTemp(n int) ([]byte, error) {
	totalLen := w.wsTmpInboundBuffer.BoundBufferSize()
	if n > totalLen {
		return nil, io.ErrShortBuffer
	} else if n <= 0 {
		n = totalLen
	}
	if w.wsTmpInboundBuffer.IsEmpty() {
		return nil, nil
	}
	head, tail := w.wsTmpInboundBuffer.Peek(n)
	w.d.reactorSub.cache.Reset()
	w.d.reactorSub.cache.Write(head)
	w.d.reactorSub.cache.Write(tail)

	data := w.d.reactorSub.cache.Bytes()
	return data, nil
}

func (w *WSSConn) discardFromWSTemp(n int) {
	_, _ = w.wsTmpInboundBuffer.Discard(n)
}

func (w *WSSConn) upgrade() error {
	buff, err := w.peekFromWSTemp(-1)
	if err != nil {
		return err
	}
	if len(buff) == 0 {
		return nil
	}

	tmpReader := bytes.NewReader(buff)
	tmpWriter := bytes.NewBuffer(nil)
	_, err = ws.Upgrade(&readWrite{
		Reader: tmpReader,
		Writer: tmpWriter,
	})
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF { //数据不完整
			return nil
		}
		w.discardFromWSTemp(len(buff)) // 发送错误，丢弃数据
		return err
	}
	_, err = w.TLSConn.Write(tmpWriter.Bytes())
	if err != nil {
		return err
	}

	w.discardFromWSTemp(len(buff) - tmpReader.Len())

	w.upgraded = true

	return nil
}

// 解包ws的数据
func (w *WSSConn) unpacketWSData() error {
	if !w.upgraded {
		return w.upgrade()
	}
	messages, err := w.decode()
	return w.stream.deliver(w.TLSConn, w.d.inboundBuffer, messages, err)
}

func (w *WSSConn) WriteServerBinary(data []byte) error { return w.writeFrame(ws.OpBinary, data) }

func (w *WSSConn) WriteServerText(data []byte) error { return w.writeFrame(ws.OpText, data) }

func (w *WSSConn) writeFrame(op ws.OpCode, data []byte) error {
	w.stream.sendMu.Lock()
	defer w.stream.sendMu.Unlock()
	if w.stream.closeSent || w.IsClosed() {
		return net.ErrClosed
	}
	var frame bytes.Buffer
	if err := wsutil.WriteServerMessage(&frame, op, data); err != nil {
		return err
	}
	_, err := w.TLSConn.Write(frame.Bytes())
	return err
}

func (w *WSSConn) CloseWithErr(err error) error { return closeWSWithErr(w.d, err) }

func (w *WSSConn) decode() ([]wsutil.Message, error) {
	buff, err := w.peekFromWSTemp(-1)
	if err != nil {
		return nil, err
	}
	return w.stream.decode(buff, w.discardFromWSTemp)
}
