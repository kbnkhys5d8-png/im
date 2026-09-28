package eventbus

import "testing"

func TestConnAdmissionVersionRemainsOwnerLocal(t *testing.T) {
	conn := &Conn{Uid: "user", NodeId: 1, ConnId: 2, SessionId: "session", AdmissionVersion: 42}
	encoded, err := conn.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var decoded Conn
	if err := decoded.Decode(encoded); err != nil {
		t.Fatal(err)
	}
	if decoded.AdmissionVersion != 0 || decoded.SessionId != conn.SessionId {
		t.Fatal("接入版本不得进入节点间编码或改变既有会话标识")
	}
}

func sessionTestConn() *Conn {
	return &Conn{Uid: "user", NodeId: 1, ConnId: 2, SessionId: "session", Uptime: 3,
		DeviceId: "device", DeviceFlag: 1, ProtoVersion: 4, AesKey: []byte("key"), AesIV: []byte("iv")}
}

func TestConnSameSession(t *testing.T) {
	if !sessionTestConn().SameSession(sessionTestConn()) {
		t.Fatal("相同会话应匹配")
	}
	tests := []struct {
		name string
		edit func(*Conn)
	}{
		{name: "用户", edit: func(c *Conn) { c.Uid = "other" }},
		{name: "节点", edit: func(c *Conn) { c.NodeId++ }},
		{name: "连接", edit: func(c *Conn) { c.ConnId++ }},
		{name: "会话", edit: func(c *Conn) { c.SessionId = "new-session" }},
		{name: "缺失会话", edit: func(c *Conn) { c.SessionId = "" }},
		{name: "时间", edit: func(c *Conn) { c.Uptime++ }},
		{name: "设备", edit: func(c *Conn) { c.DeviceId = "new-device" }},
		{name: "设备类型", edit: func(c *Conn) { c.DeviceFlag++ }},
		{name: "协议", edit: func(c *Conn) { c.ProtoVersion++ }},
		{name: "JSONRPC", edit: func(c *Conn) { c.IsJsonRpc = true }},
		{name: "密钥", edit: func(c *Conn) { c.AesKey = []byte("new-key") }},
		{name: "向量", edit: func(c *Conn) { c.AesIV = []byte("new-iv") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := sessionTestConn()
			tt.edit(current)
			if sessionTestConn().SameSession(current) || current.SameSession(sessionTestConn()) {
				t.Fatal("不同会话不应匹配")
			}
		})
	}
	var absent *Conn
	if absent.SameSession(nil) || sessionTestConn().SameSession(nil) {
		t.Fatal("空连接不应匹配")
	}
	legacyA, legacyB := sessionTestConn(), sessionTestConn()
	legacyA.SessionId, legacyB.SessionId = "", ""
	if !legacyA.SameSession(legacyB) {
		t.Fatal("双方旧编码仍应兼容原会话字段")
	}
	current := sessionTestConn()
	current.LastActive = 99
	current.OutMsgCount.Add(7)
	current.Auth = true
	if !sessionTestConn().SameSession(current) {
		t.Fatal("活跃时间、认证进度与统计不是会话身份")
	}
}

func TestConnSameSessionWithoutEncryptionUsesGeneration(t *testing.T) {
	for _, jsonRPC := range []bool{false, true} {
		old := &Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "old", IsJsonRpc: jsonRPC}
		current := &Conn{Uid: "user", NodeId: 1, ConnId: 2, Uptime: 3, SessionId: "new", IsJsonRpc: jsonRPC}
		if old.SameSession(current) {
			t.Fatal("无加密会话也不能因相同秒内复用连接 ID 而匹配")
		}
	}
}

func TestConnSessionEncodingCompatibility(t *testing.T) {
	conn := sessionTestConn()
	data, err := conn.Encode()
	if err != nil {
		t.Fatal(err)
	}
	var decoded Conn
	if err := decoded.Decode(data); err != nil {
		t.Fatal(err)
	}
	if !conn.SameSession(&decoded) {
		t.Fatal("内部转发必须保留会话标识与握手字段")
	}
	legacy := sessionTestConn()
	legacy.SessionId = ""
	legacyData, err := legacy.Encode()
	if err != nil {
		t.Fatal(err)
	}
	for _, oldData := range [][]byte{legacyData, legacyData[:len(legacyData)-1]} {
		if err := decoded.Decode(oldData); err != nil {
			t.Fatal(err)
		}
		if decoded.SessionId != "" {
			t.Fatal("解码旧数据必须清除上次的会话标识")
		}
	}
	if err := decoded.Decode(data[:len(data)-1]); err == nil {
		t.Fatal("截断的会话标识不能被当成有效旧编码")
	}
}
