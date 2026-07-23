package wknet

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEngine(t *testing.T) {
	e := NewEngine()

	e.Start()
	defer e.Stop()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	clientCount := 100
	clientMsgCount := 5

	var wg sync.WaitGroup

	wg.Add(clientCount * clientMsgCount)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	e.OnData(func(conn Conn) error {
		buffs, err := conn.Peek(-1)
		assert.NoError(t, err)
		lastNewline := bytes.LastIndexByte(buffs, '\n')
		if lastNewline < 0 {
			return nil
		}

		complete := buffs[:lastNewline+1]
		_, err = conn.Discard(len(complete))
		assert.NoError(t, err)
		buffStrs := strings.Split(strings.TrimSuffix(string(complete), "\n"), "\n")
		for _, buff := range buffStrs {
			value := buff
			values := strings.Split(value, " ")
			cmd := values[0]
			param := values[1:]
			if !conn.IsAuthed() {
				if cmd == "auth" {
					conn.SetAuthed(true)
					conn.SetUID(param[0])
					conn.WriteToOutboundBuffer([]byte("auth ok\n"))
					conn.WakeWrite()
				} else {
					panic("need auth")
				}
			} else if cmd == "send" {
				go func(c Conn, p []string) {
					assert.Equal(t, p[0], c.UID())
					wg.Done()

				}(conn, param)

			}
		}

		return nil
	})

	time.Sleep(time.Millisecond * 100)
	for i := 0; i < clientCount; i++ {
		uid := fmt.Sprintf("uid%d", i)
		cli, err := net.Dial("tcp", e.TCPRealListenAddr().String())
		assert.NoError(t, err)
		_, err = cli.Write([]byte(fmt.Sprintf("auth %s\n", uid)))
		assert.NoError(t, err)

		_, _, err = bufio.NewReader(cli).ReadLine()
		assert.NoError(t, err)
		defer cli.Close()
		go func(c net.Conn) {
			for j := 0; j < clientMsgCount; j++ {
				c.Write([]byte(fmt.Sprintf("send %s\n", uid)))
			}
		}(cli)
	}
	select {
	case <-done:
	case <-timeoutCtx.Done():
		t.Fatal("timed out waiting for engine messages")
	}
}
