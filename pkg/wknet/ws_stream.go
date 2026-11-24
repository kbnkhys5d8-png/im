package wknet

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
)

type wsStream struct {
	op      ws.OpCode
	partial []byte
	limit   int
	done    bool

	sendMu    sync.Mutex
	closeSent bool
}

// 每条连接单独保存分片；只消费完整的帧，控制帧不必等待数据消息的最后一片。
func (s *wsStream) decode(buf []byte, discard func(int)) (messages []wsutil.Message, err error) {
	if s.done {
		discard(len(buf))
		return nil, nil
	}
	r := bytes.NewReader(buf)
	consumed := 0
	defer func() {
		if err != nil || s.done {
			s.done, s.partial = true, nil
			consumed = len(buf)
		}
		if consumed > 0 {
			discard(consumed)
		}
	}()
	for r.Len() >= ws.MinHeaderSize {
		h, err := ws.ReadHeader(r)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return messages, err
		}
		state := ws.StateServerSide
		if s.op != 0 {
			state = state.Set(ws.StateFragmented)
		}
		if err := ws.CheckHeader(h, state); err != nil {
			return messages, err
		}
		lengthCode := buf[consumed+1] & 0x7f
		if (lengthCode == 126 && h.Length < 126) || (lengthCode == 127 && h.Length < 65536) || (h.OpCode == ws.OpClose && h.Length == 1) {
			return messages, ws.ProtocolError("无效的帧长度")
		}
		fragment := !h.OpCode.IsControl() && (!h.Fin || h.OpCode == ws.OpContinuation)
		if fragment && s.limit > 0 && h.Length > int64(s.limit-len(s.partial)) {
			return messages, ws.ProtocolError("分片消息超过接收上限")
		}
		if h.Length > int64(r.Len()) {
			break
		}
		start := len(buf) - r.Len()
		_, _ = r.Seek(h.Length, io.SeekCurrent)
		consumed = len(buf) - r.Len()
		part := buf[start:consumed]
		op := h.OpCode
		var payload []byte
		if fragment {
			if op != ws.OpContinuation {
				s.op = op
			}
			offset := len(s.partial)
			s.partial = append(s.partial, part...)
			ws.Cipher(s.partial[offset:], h.Mask, 0)
			if !h.Fin {
				continue
			}
			op, payload = s.op, s.partial
			s.op, s.partial = 0, nil
		} else {
			payload = bytes.Clone(part)
			ws.Cipher(payload, h.Mask, 0)
		}
		if op == ws.OpText && !utf8.Valid(payload) {
			return messages, ws.ProtocolError("文本消息不是有效的 UTF-8")
		}
		messages = append(messages, wsutil.Message{OpCode: op, Payload: payload})
		if op == ws.OpClose {
			s.done = true
			break
		}
	}
	return messages, nil
}

func (s *wsStream) deliver(dst io.Writer, in InboundBuffer, messages []wsutil.Message, terminal error) error {
	delivered := false
	for _, m := range messages {
		var err error
		switch {
		case m.OpCode == ws.OpClose:
			s.sendMu.Lock()
			s.closeSent = true
			err = wsutil.HandleClientControlMessage(dst, m)
			s.sendMu.Unlock()
		case m.OpCode.IsControl():
			err = wsutil.HandleClientControlMessage(dst, m)
		case len(m.Payload) != 0:
			_, err = in.Write(m.Payload)
			delivered = delivered || err == nil
		}
		if err != nil {
			terminal = err
			break
		}
	}
	if terminal != nil && delivered {
		return &readErrorWithData{err: terminal}
	}
	return terminal
}

func closeWSWithErr(d *DefaultConn, err error) error {
	var closed wsutil.ClosedError
	if !errors.As(err, &closed) {
		return d.CloseWithErr(err)
	}
	d.mu.Lock()
	if d.closed.Load() || d.outboundBuffer.IsEmpty() {
		d.mu.Unlock()
		return d.Close()
	}
	// Close 应答可能还在发送队列。停止读取，等可写事件排空；对端不读时也要回收连接。
	if d.wsCloseTimer == nil {
		d.wsCloseTimer = time.AfterFunc(time.Second, func() { _ = d.Close() })
		_ = d.reactorSub.RemoveRead(d)
	}
	d.mu.Unlock()
	return d.Flush()
}
