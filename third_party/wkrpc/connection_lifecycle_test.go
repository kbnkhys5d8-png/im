package wkrpc

import (
	"errors"
	"testing"

	"github.com/WuKongIM/wkrpc/proto"
	"github.com/panjf2000/gnet/v2"
)

func TestConnectionAuthorizerRunsBeforeReservedUIDReplacement(t *testing.T) {
	const uid = "wk.plugin.search"
	existing := &lifecycleTestConn{fd: 10}
	incoming := &lifecycleTestConn{fd: 11}
	var server *Server
	server = New("unix:///tmp/not-started.sock", WithConnectionAuthorizer(func(gotUID string, gotConn gnet.Conn) error {
		if gotUID != uid || gotConn != incoming {
			t.Fatalf("authorizer received uid=%q conn=%p", gotUID, gotConn)
		}
		if got := server.ConnManager.GetConn(uid); got != existing {
			t.Fatal("reserved UID was replaced before connection authorization")
		}
		return errors.New("unmanaged peer")
	}))
	server.ConnManager.AddConn(uid, existing)

	server.handleConnack(incoming, &proto.Connect{Uid: uid})

	if got := server.ConnManager.GetConn(uid); got != existing {
		t.Fatal("rejected peer replaced the authoritative connection")
	}
	if !incoming.closed {
		t.Fatal("rejected peer connection remained open")
	}
	if incoming.Context() != nil {
		t.Fatal("rejected peer received an authenticated connection context")
	}
}

func TestConnectionAuthorizerAllowsOrdinaryUID(t *testing.T) {
	const uid = "ordinary.plugin"
	incoming := &lifecycleTestConn{fd: 12}
	authorizerCalls := 0
	server := New("unix:///tmp/not-started.sock", WithConnectionAuthorizer(func(gotUID string, gotConn gnet.Conn) error {
		authorizerCalls++
		if gotUID != uid || gotConn != incoming {
			t.Fatalf("authorizer received uid=%q conn=%p", gotUID, gotConn)
		}
		return nil
	}))

	server.handleConnack(incoming, &proto.Connect{Uid: uid})

	if authorizerCalls != 1 {
		t.Fatalf("authorizer calls = %d, want 1", authorizerCalls)
	}
	if got := server.ConnManager.GetConn(uid); got != incoming {
		t.Fatal("authorized ordinary UID was not registered")
	}
	if incoming.closed || incoming.Context() == nil {
		t.Fatal("authorized ordinary UID was rejected")
	}
}

func TestStaleCloseDoesNotRemoveOrNotifyReplacementConnection(t *testing.T) {
	const uid = "wk.plugin.search"
	server := New("unix:///tmp/not-started.sock")
	oldConn := &lifecycleTestConn{fd: 10, context: newConnContext(uid)}
	currentConn := &lifecycleTestConn{fd: 11, context: newConnContext(uid)}
	server.ConnManager.AddConn(uid, oldConn)
	server.ConnManager.AddConn(uid, currentConn)
	closeCalls := 0
	server.Route(server.opts.ClosePath, func(*Context) { closeCalls++ })

	server.OnClose(oldConn, nil)

	if got := server.ConnManager.GetConn(uid); got != currentConn {
		t.Fatal("stale close removed the replacement connection")
	}
	if closeCalls != 0 {
		t.Fatalf("stale close invoked close route %d times", closeCalls)
	}
}

type lifecycleTestConn struct {
	gnet.Conn
	fd      int
	context any
	closed  bool
}

func (c *lifecycleTestConn) Fd() int                                     { return c.fd }
func (c *lifecycleTestConn) Context() any                                { return c.context }
func (c *lifecycleTestConn) SetContext(v any)                            { c.context = v }
func (c *lifecycleTestConn) Close() error                                { c.closed = true; return nil }
func (c *lifecycleTestConn) AsyncWrite([]byte, gnet.AsyncCallback) error { return nil }
