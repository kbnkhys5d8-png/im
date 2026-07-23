package wknet

import (
	"bytes"
	stdtls "crypto/tls"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	wktls "github.com/WuKongIM/crypto/tls"
	"github.com/stretchr/testify/require"
)

type bufferedTLSRecordConn struct {
	net.Conn

	mu      sync.Mutex
	buffer  bool
	pending bytes.Buffer
}

func (c *bufferedTLSRecordConn) Write(data []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.buffer {
		return c.pending.Write(data)
	}
	return c.Conn.Write(data)
}

func (c *bufferedTLSRecordConn) startBuffering() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buffer = true
}

func (c *bufferedTLSRecordConn) flushWithMalformedRecord() error {
	c.mu.Lock()
	data := append([]byte(nil), c.pending.Bytes()...)
	c.pending.Reset()
	c.buffer = false
	c.mu.Unlock()

	// TLS application-data header with a one-byte payload. Once the valid
	// encrypted record immediately before it is consumed, this record fails
	// authentication.
	data = append(data, 0x17, 0x03, 0x03, 0x00, 0x01, 0x00)
	_, err := c.Conn.Write(data)
	return err
}

func TestTLSConnDeliversPlaintextThenClosesOnFatalRecord(t *testing.T) {
	logOptions := wklog.NewOptions()
	logOptions.LogDir = t.TempDir()
	logOptions.NoStdout = true
	wklog.Configure(logOptions)

	cert, err := wktls.X509KeyPair(rsaCertPEM, rsaKeyPEM)
	require.NoError(t, err)

	engine := NewEngine(
		WithAddr("tcp://127.0.0.1:0"),
		WithTCPTLSConfig(&wktls.Config{
			Certificates: []wktls.Certificate{cert},
			MaxVersion:   wktls.VersionTLS12,
		}),
	)

	delivered := make(chan string, 1)
	closed := make(chan struct{}, 1)
	engine.OnData(func(conn Conn) error {
		data, err := conn.Peek(-1)
		require.NoError(t, err)
		if len(data) == 0 {
			return nil
		}
		_, err = conn.Discard(len(data))
		require.NoError(t, err)
		delivered <- string(data)
		return nil
	})
	engine.OnClose(func(Conn) {
		closed <- struct{}{}
	})

	require.NoError(t, engine.Start())
	defer engine.Stop()

	raw, err := net.Dial("tcp", engine.TCPRealListenAddr().String())
	require.NoError(t, err)
	buffered := &bufferedTLSRecordConn{Conn: raw}
	client := stdtls.Client(buffered, &stdtls.Config{
		InsecureSkipVerify: true,
		MaxVersion:         stdtls.VersionTLS12,
	})
	require.NoError(t, client.Handshake())
	defer client.Close()

	buffered.startBuffering()
	_, err = client.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, buffered.flushWithMalformedRecord())

	select {
	case got := <-delivered:
		require.Equal(t, "hello", got)
	case <-time.After(2 * time.Second):
		t.Fatal("valid plaintext was not delivered before the fatal TLS record")
	}

	select {
	case <-closed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("connection stayed open after a fatal TLS record")
	}
}
