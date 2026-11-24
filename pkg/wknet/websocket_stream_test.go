package wknet

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	wktls "github.com/WuKongIM/crypto/tls"
	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestWebSocketStreamControlsAndTerminalReads(t *testing.T) {
	certHost := httptest.NewTLSServer(nil)
	cert := certHost.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(certHost.Certificate())
	certHost.Close()
	dialer := websocket.Dialer{TLSClientConfig: &tls.Config{RootCAs: roots}, HandshakeTimeout: 3 * time.Second}
	dialer.NetDialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		if err == nil {
			err = c.(*net.TCPConn).SetReadBuffer(32 << 10)
			if err != nil {
				_ = c.Close()
			}
		}
		return c, err
	}
	for _, scheme := range []string{"ws", "wss"} {
		t.Run(scheme, func(t *testing.T) {
			listen := WithWSAddr("ws://127.0.0.1:0")
			if scheme == "wss" {
				listen = WithWSSAddr("wss://127.0.0.1:0")
			}
			engine := NewEngine(WithAddr("tcp://127.0.0.1:0"), listen, WithSubReactorNum(1), WithSocketSendBuffer(16<<10),
				WithWSTLSConfig(&wktls.Config{Certificates: []wktls.Certificate{{Certificate: cert.Certificate, PrivateKey: cert.PrivateKey}}}))
			events := make(chan string, 16)
			queued := make(chan *DefaultConn, 1)
			backlog := bytes.Repeat([]byte("x"), 512<<10)
			engine.OnData(func(c Conn) error {
				b, err := c.Peek(-1)
				if err != nil || len(b) == 0 {
					return err
				}
				msg := string(b)
				_, err = c.Discard(len(b))
				events <- "data:" + msg
				switch msg {
				case "queue", "stall":
					payload := backlog
					if msg == "stall" {
						payload = bytes.Repeat(backlog, 16)
					}
					if err := c.(IWSConn).WriteServerBinary(payload); err != nil {
						return err
					}
					queued <- defaultConnOf(c)
					return c.WakeWrite()
				case "before":
					// 同批 Close 已限制新的应用写入；回调报错也不能覆盖原本的关闭握手。
					var s *wsStream
					switch w := c.(type) {
					case *WSConn:
						s = &w.stream
					case *WSSConn:
						s = &w.stream
					}
					s.sendMu.Lock()
					closing := s.closeSent
					s.sendMu.Unlock()
					if closing {
						return c.(IWSConn).WriteServerText([]byte("late"))
					}
				}
				return err
			})
			engine.OnClose(func(Conn) { events <- "closed" })
			require.NoError(t, engine.Start())
			t.Cleanup(func() { _ = engine.Stop() })
			var addr net.Addr
			if scheme == "wss" {
				addr = engine.WSSRealListenAddr()
			} else {
				addr = engine.WSRealListenAddr()
			}
			wantEvent := func(t *testing.T, want string) {
				t.Helper()
				select {
				case got := <-events:
					require.Equal(t, want, got)
				case <-time.After(2 * time.Second):
					t.Fatalf("未收到事件 %q", want)
				}
			}
			for _, tc := range []string{"分片未结束时回心跳和关闭", "错误前的完整消息只交付一次", "有积压仍发送关闭应答", "对端不读也能关闭"} {
				t.Run(tc, func(t *testing.T) {
					c, _, err := dialer.Dial(scheme+"://"+addr.String(), nil)
					require.NoError(t, err)
					t.Cleanup(func() { _ = c.Close() })
					sock := c.UnderlyingConn()
					require.NoError(t, sock.SetDeadline(time.Now().Add(3*time.Second)))
					send := func(frames ...ws.Frame) {
						t.Helper()
						var wire bytes.Buffer
						for _, f := range frames {
							require.NoError(t, ws.WriteFrame(&wire, ws.MaskFrame(f)))
						}
						_, err := sock.Write(wire.Bytes())
						require.NoError(t, err)
					}
					if tc == "有积压仍发送关闭应答" || tc == "对端不读也能关闭" {
						req := "queue"
						if tc == "对端不读也能关闭" {
							req = "stall"
						}
						send(ws.NewBinaryFrame([]byte(req)))
						wantEvent(t, "data:"+req)
						var d *DefaultConn
						select {
						case d = <-queued:
						case <-time.After(2 * time.Second):
							t.Fatal("数据未入发送队列")
						}
						send(ws.NewCloseFrame(nil))
						if req == "stall" {
							require.Eventually(t, func() bool { d.mu.Lock(); defer d.mu.Unlock(); return d.wsCloseTimer != nil }, time.Second, time.Millisecond)
						}
						if tc == "有积压仍发送关闭应答" {
							f, err := ws.ReadFrame(sock)
							require.NoError(t, err)
							require.Equal(t, ws.OpBinary, f.Header.OpCode)
							require.Equal(t, backlog, f.Payload)
							f, err = ws.ReadFrame(sock)
							require.NoError(t, err)
							require.Equal(t, ws.OpClose, f.Header.OpCode)
						}
						wantEvent(t, "closed")
						return
					}
					if tc == "错误前的完整消息只交付一次" {
						send(ws.NewBinaryFrame([]byte("one")), ws.NewBinaryFrame([]byte("two")), ws.NewFrame(ws.OpContinuation, true, nil), ws.NewBinaryFrame([]byte("after")))
						got := ""
						for got != "onetwo" {
							select {
							case e := <-events:
								require.Contains(t, e, "data:")
								got += e[len("data:"):]
								require.Contains(t, "onetwo", got)
							case <-time.After(2 * time.Second):
								t.Fatal("关闭前未交付完整数据")
							}
						}
						wantEvent(t, "closed")
						return
					}
					// 不发最后一片，先在真实套接字读取 Pong，排除整批解完才响应。
					send(ws.NewFrame(ws.OpBinary, false, []byte("a")), ws.NewPingFrame([]byte("p")))
					f, err := ws.ReadFrame(sock)
					require.NoError(t, err)
					require.Equal(t, ws.OpPong, f.Header.OpCode)
					require.Equal(t, []byte("p"), f.Payload)
					send(ws.NewFrame(ws.OpContinuation, true, []byte("b")))
					wantEvent(t, "data:ab")
					send(ws.NewBinaryFrame([]byte("before")), ws.NewFrame(ws.OpBinary, false, []byte("unfinished")), ws.NewCloseFrame(ws.NewCloseFrameBody(ws.StatusNormalClosure, "")))
					f, err = ws.ReadFrame(sock)
					require.NoError(t, err)
					require.Equal(t, ws.OpClose, f.Header.OpCode)
					code, _ := ws.ParseCloseFrameData(f.Payload)
					require.Equal(t, ws.StatusNormalClosure, code)
					wantEvent(t, "data:before")
					wantEvent(t, "closed")
				})
				if t.Failed() {
					return
				}
			}
		})
	}
}

func TestWebSocketStreamReadBoundaries(t *testing.T) {
	ack := func(id int64) []byte {
		b, err := wkproto.New().EncodeFrame(&wkproto.RecvackPacket{MessageID: id, MessageSeq: uint32(id)}, 4)
		require.NoError(t, err)
		return b
	}
	a, b := ack(1), ack(2)
	long := bytes.Repeat([]byte("x"), 160)
	for _, tc := range []struct {
		name   string
		frames []ws.Frame
		want   []wsutil.Message
	}{
		{"两个回执", []ws.Frame{ws.NewBinaryFrame(a), ws.NewBinaryFrame(b)}, []wsutil.Message{
			{OpCode: ws.OpBinary, Payload: a}, {OpCode: ws.OpBinary, Payload: b},
		}},
		{"心跳和扩展长度", []ws.Frame{ws.NewPingFrame(a), ws.NewBinaryFrame(long)}, []wsutil.Message{
			{OpCode: ws.OpPing, Payload: a}, {OpCode: ws.OpBinary, Payload: long},
		}},
		{"分片中夹心跳", []ws.Frame{ws.NewFrame(ws.OpBinary, false, a), ws.NewPingFrame(b), ws.NewFrame(ws.OpContinuation, true, b)}, []wsutil.Message{
			{OpCode: ws.OpPing, Payload: b}, {OpCode: ws.OpBinary, Payload: bytes.Join([][]byte{a, b}, nil)},
		}},
	} {
		var wire bytes.Buffer
		for _, f := range tc.frames {
			require.NoError(t, ws.WriteFrame(&wire, ws.MaskFrame(f)))
		}
		for _, secure := range []bool{false, true} {
			name := "WS"
			if secure {
				name = "WSS"
			}
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				for split := 0; split <= wire.Len(); split++ {
					buf := NewDefaultBuffer()
					base := &DefaultConn{reactorSub: &ReactorSub{}, Log: wklog.NewWKLog("流读取测试")}
					decode := (&WSConn{DefaultConn: base, tmpInboundBuffer: buf}).decode
					if secure {
						decode = (&WSSConn{TLSConn: &TLSConn{d: base}, wsTmpInboundBuffer: buf}).decode
					}
					var got []wsutil.Message
					for _, part := range [][]byte{wire.Bytes()[:split], wire.Bytes()[split:]} {
						_, err := buf.Write(part)
						require.NoError(t, err)
						m, err := decode()
						require.NoError(t, err, "读取边界 %d", split)
						got = append(got, m...)
					}
					require.Equal(t, tc.want, got, "读取边界 %d", split)
					require.True(t, buf.IsEmpty(), "读取边界 %d", split)
					require.NoError(t, buf.Release())
				}
			})
		}
	}
}

func TestWebSocketStreamFragmentValidation(t *testing.T) {
	word := []byte("群")
	for _, tc := range []struct {
		name      string
		last      ws.Frame
		wantError bool
	}{
		{"跨片的完整文字", ws.NewFrame(ws.OpContinuation, true, word[1:]), false},
		{"不完整的文字", ws.NewFrame(ws.OpContinuation, true, nil), true},
		{"累计分片超限", ws.NewFrame(ws.OpContinuation, true, []byte("abcd")), true},
		{"分片中出现新数据帧", ws.NewBinaryFrame(nil), true},
		{"无效的关闭帧", ws.NewCloseFrame([]byte{0}), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := wsStream{limit: len(word)}
			var wire bytes.Buffer
			for _, f := range []ws.Frame{ws.NewBinaryFrame([]byte("ok")), ws.NewFrame(ws.OpText, false, word[:1]), ws.NewPongFrame(nil), tc.last} {
				require.NoError(t, ws.WriteFrame(&wire, ws.MaskFrame(f)))
			}
			m, err := s.decode(wire.Bytes(), func(n int) { wire.Next(n) })
			if tc.wantError {
				require.Error(t, err)
				require.Len(t, m, 2)
				require.True(t, s.done)
			} else {
				require.NoError(t, err)
				require.Len(t, m, 3)
				require.Equal(t, ws.OpText, m[2].OpCode)
				require.Equal(t, word, m[2].Payload)
			}
			require.Equal(t, []byte("ok"), m[0].Payload)
			require.Equal(t, ws.OpPong, m[1].OpCode)
			require.Empty(t, s.partial)
			require.Zero(t, wire.Len())
		})
	}
}
