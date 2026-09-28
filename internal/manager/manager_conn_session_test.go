package manager

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wknet"
)

type managedSessionTestConn struct {
	wknet.Conn
	id int64
}

func (c *managedSessionTestConn) ID() int64 { return c.id }

func TestConnManagerLateRemoveDoesNotDeleteReusedID(t *testing.T) {
	m := NewConnManager(1, nil)
	old := &managedSessionTestConn{id: 9}
	fresh := &managedSessionTestConn{id: 9}
	m.AddConn(old)
	m.AddConn(fresh)
	m.RemoveConn(old)
	if m.GetConn(9) != fresh {
		t.Fatal("旧连接的延迟移除不得删除同 ID 的新 socket")
	}
	m.RemoveConn(fresh)
	if m.GetConn(9) != nil {
		t.Fatal("当前连接应可以正常移除")
	}
}
