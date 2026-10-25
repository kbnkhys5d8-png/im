package eventbus

import (
	"testing"
)

func TestConnectionTokenVersionInternalEncodingIsBackwardCompatible(t *testing.T) {
	conn := &Conn{Uid: "u", SessionId: "s", AuthTokenHash: TokenFingerprint("t"), AuthTokenVersion: 43}
	wire, err := conn.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var decoded Conn
	if err := decoded.Decode(wire); err != nil {
		t.Fatal(err)
	}
	if decoded.AuthTokenVersion != 43 || !decoded.AuthTokenVersionKnown || decoded.AuthTokenHash != conn.AuthTokenHash || !decoded.SameSession(conn) {
		t.Fatal("内部连接代际丢失")
	}
	// 旧服务端事件缺少追加版本字段，复用对象解码也必须回到零值。
	if err := decoded.Decode(wire[:len(wire)-8]); err != nil {
		t.Fatal(err)
	}
	if decoded.AuthTokenVersion != 0 || decoded.AuthTokenVersionKnown || decoded.AuthTokenHash != conn.AuthTokenHash {
		t.Fatal("旧事件被错误赋予新代际")
	}
	conn.AuthTokenVersion = 0
	conn.AuthTokenVersionKnown = true
	wire, err = conn.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := decoded.Decode(wire); err != nil {
		t.Fatal(err)
	}
	if !decoded.AuthTokenVersionKnown || decoded.AuthTokenVersion != 0 {
		t.Fatal("已验证的旧版零版本不能混同为未知代际")
	}
}
