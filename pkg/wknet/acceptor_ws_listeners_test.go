package wknet

import (
	"crypto/tls"
	"crypto/x509"
	"net/http/httptest"
	"testing"
	"time"

	wktls "github.com/WuKongIM/crypto/tls"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

func TestWebSocketListenersStartTogether(t *testing.T) {
	certHost := httptest.NewTLSServer(nil)
	cert := certHost.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(certHost.Certificate())
	certHost.Close()

	engine := NewEngine(
		WithAddr("tcp://127.0.0.1:0"),
		WithWSAddr("ws://127.0.0.1:0"),
		WithWSSAddr("wss://127.0.0.1:0"),
		WithWSTLSConfig(&wktls.Config{Certificates: []wktls.Certificate{{Certificate: cert.Certificate, PrivateKey: cert.PrivateKey}}}),
		WithSubReactorNum(1),
	)
	engine.OnData(func(c Conn) error {
		data, err := c.Peek(-1)
		if err != nil || len(data) == 0 {
			return err
		}
		// 一并回传服务端看到的本地地址，验证连接没有拿到另一监听器的端口。
		reply := append([]byte(c.LocalAddr().String()+"/"), data...)
		if _, err := c.Discard(len(data)); err != nil {
			return err
		}
		if err := c.(IWSConn).WriteServerBinary(reply); err != nil {
			return err
		}
		return c.WakeWrite()
	})
	require.NoError(t, engine.Start())
	t.Cleanup(func() { require.NoError(t, engine.Stop()) })

	plain, secure := engine.WSRealListenAddr().String(), engine.WSSRealListenAddr().String()
	require.NotEqual(t, plain, secure)
	dialer := websocket.Dialer{TLSClientConfig: &tls.Config{RootCAs: roots}, HandshakeTimeout: 3 * time.Second}
	for _, tt := range []struct{ scheme, addr string }{{"ws", plain}, {"wss", secure}} {
		t.Run(tt.scheme, func(t *testing.T) {
			c, _, err := dialer.Dial(tt.scheme+"://"+tt.addr, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Close() })
			deadline := time.Now().Add(3 * time.Second)
			require.NoError(t, c.SetWriteDeadline(deadline))
			require.NoError(t, c.SetReadDeadline(deadline))
			require.NoError(t, c.WriteMessage(websocket.BinaryMessage, []byte(tt.scheme)))
			typ, reply, err := c.ReadMessage()
			require.NoError(t, err)
			require.Equal(t, websocket.BinaryMessage, typ)
			require.Equal(t, tt.addr+"/"+tt.scheme, string(reply))
		})
	}
}
