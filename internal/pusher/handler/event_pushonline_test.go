package handler

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestProcessChannelPushTargetSessions(t *testing.T) {
	for _, frameName := range []string{"消息", "事件"} {
		t.Run(frameName, func(t *testing.T) {
			oldUser, oldOptions := eventbus.User, options.G
			t.Cleanup(func() { eventbus.User, options.G = oldUser, oldOptions })
			options.G = &options.Options{DisableEncryption: true}
			tests := []struct {
				name    string
				targets func() []*eventbus.Conn
				want    []int64
			}{
				{name: "nil保留全量推送", want: []int64{1, 2}},
				{name: "空集合不回退全量", targets: func() []*eventbus.Conn {
					return []*eventbus.Conn{}
				}},
				{name: "只投当前指定会话", targets: func() []*eventbus.Conn {
					return []*eventbus.Conn{pushTestConn("receiver", 2, "session-2")}
				}, want: []int64{2}},
				{name: "目标重复只发一次", targets: func() []*eventbus.Conn {
					return []*eventbus.Conn{
						pushTestConn("receiver", 2, "session-2"),
						pushTestConn("receiver", 2, "session-2"),
					}
				}, want: []int64{2}},
				{name: "旧代次不命中新设备", targets: func() []*eventbus.Conn {
					return []*eventbus.Conn{pushTestConn("receiver", 2, "old-session")}
				}},
				{name: "其他用户不可投递", targets: func() []*eventbus.Conn {
					return []*eventbus.Conn{pushTestConn("other", 2, "session-2")}
				}},
				{name: "未经认证的目标不可投递", targets: func() []*eventbus.Conn {
					conn := pushTestConn("receiver", 2, "session-2")
					conn.Auth = false
					return []*eventbus.Conn{conn}
				}},
				{name: "缺失会话代次不匹配新会话", targets: func() []*eventbus.Conn {
					return []*eventbus.Conn{pushTestConn("receiver", 2, "")}
				}},
				{name: "不同节点不可投递", targets: func() []*eventbus.Conn {
					conn := pushTestConn("receiver", 2, "session-2")
					conn.NodeId++
					return []*eventbus.Conn{conn}
				}},
				{name: "目录已移除不可投递", targets: func() []*eventbus.Conn {
					return []*eventbus.Conn{pushTestConn("receiver", 3, "session-3")}
				}},
				{name: "目录未认证不可投递", targets: func() []*eventbus.Conn {
					return []*eventbus.Conn{pushTestConn("receiver", 4, "session-4")}
				}},
				{name: "空连接忽略且不影响有效目标", targets: func() []*eventbus.Conn {
					return []*eventbus.Conn{nil, pushTestConn("receiver", 1, "session-1")}
				}, want: []int64{1}},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					first := pushTestConn("receiver", 1, "session-1")
					second := pushTestConn("receiver", 2, "session-2")
					unauthed := pushTestConn("receiver", 4, "session-4")
					unauthed.Auth = false
					users := &pushSessionUsers{conns: []*eventbus.Conn{first, second, unauthed}}
					eventbus.RegisterUser(users)
					event := &eventbus.Event{
						Type: eventbus.EventPushOnline, ToUid: "receiver",
						Conn:  pushTestConn("sender", 10, "sender-session"),
						Frame: &wkproto.SendPacket{Framer: wkproto.Framer{NoPersist: true}, Payload: []byte("test")},
					}
					if frameName == "事件" {
						event.Frame = &wkproto.EventPacket{}
					}
					if tt.targets != nil {
						event.ToConns = tt.targets()
					}
					(&Handler{}).processChannelPush([]*eventbus.Event{event})
					if len(users.writes) != len(tt.want) {
						t.Fatalf("投递 %d 次，期望 %d 次", len(users.writes), len(tt.want))
					}
					for i, id := range tt.want {
						written := users.writes[i]
						if written.Type != eventbus.EventConnWriteFrame || written.Conn.ConnId != id {
							t.Fatalf("投递事件 %d = %#v，期望连接 %d", i, written, id)
						}
						if written.Conn != users.conns[id-1] {
							t.Fatal("必须使用当前目录连接，不应使用补投目标的旧指针")
						}
					}
				})
			}
		})
	}
}

func TestPushEventTargetSessionsCloneAndEncoding(t *testing.T) {
	conn := pushTestConn("receiver", 1, "session-1")
	tests := []struct {
		name    string
		targets []*eventbus.Conn
	}{
		{name: "未指定"},
		{name: "指定空集合", targets: []*eventbus.Conn{}},
		{name: "指定会话", targets: []*eventbus.Conn{conn}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := &eventbus.Event{Type: eventbus.EventPushOnline, ToUid: "receiver", ToConns: tt.targets}
			clone := event.Clone()
			if (clone.ToConns == nil) != (event.ToConns == nil) || len(clone.ToConns) != len(event.ToConns) {
				t.Fatal("Clone 必须保留 nil 与显式空集合的不同语义")
			}
			if len(event.ToConns) > 0 {
				if clone.ToConns[0] != conn {
					t.Fatal("Clone 必须保留指定会话")
				}
				clone.ToConns[0] = nil
				if event.ToConns[0] != conn {
					t.Fatal("Clone 不应共享可变的目标切片")
				}
			}
			encoded, err := (eventbus.EventBatch{event}).Encode()
			if err != nil {
				t.Fatal(err)
			}
			plain := &eventbus.Event{Type: event.Type, ToUid: event.ToUid}
			plainEncoded, err := (eventbus.EventBatch{plain}).Encode()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded, plainEncoded) || event.Size() != plain.Size() {
				t.Fatal("定向会话只保留在进程内，不得改变现有编码或编码大小")
			}
			var decoded eventbus.EventBatch
			if err := decoded.Decode(encoded); err != nil {
				t.Fatal(err)
			}
			if len(decoded) != 1 || decoded[0].ToConns != nil {
				t.Fatal("节点间解码不得携带进程内补投目标")
			}
		})
	}
}

func TestEncryptMessagePayloadPreservesInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		size     int
		capacity int
	}{
		{name: "nil消息"},
		{name: "空消息保留容量", capacity: 32},
		{name: "单字节保留容量", size: 1, capacity: 32},
		{name: "块边界前保留容量", size: 15, capacity: 32},
		{name: "完整块保留容量", size: 16, capacity: 32},
		{name: "块边界后保留容量", size: 17, capacity: 64},
		{name: "多块保留容量", size: 32, capacity: 64},
		{name: "容量不足填充", size: 17, capacity: 20},
		{name: "无多余容量", size: 17, capacity: 17},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var backing []byte
			if tt.capacity > 0 {
				// 尾部哨兵用于确认加密没有改写共享数组的预留容量。
				backing = bytes.Repeat([]byte{0xa5}, tt.capacity)
			}
			payload := backing[:tt.size]
			for i := range payload {
				payload[i] = byte(i + 1)
			}
			before := bytes.Clone(backing)
			conn := &eventbus.Conn{AesKey: []byte("0123456789abcdef"), AesIV: []byte("abcdef0123456789")}
			// 用独占且无预留容量的输入核对既有密文格式。
			independent := make([]byte, len(payload))
			copy(independent, payload)
			want, err := wkutil.AesEncryptPkcs7Base64(independent, conn.AesKey, conn.AesIV)
			if err != nil {
				t.Fatal(err)
			}
			got, err := encryptMessagePayload(payload, conn)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatal("加密结果不得改变既有密文格式")
			}
			if !bytes.Equal(backing, before) {
				t.Fatal("加密修改了共享消息底层数组或其预留容量")
			}
			plain, err := wkutil.AesDecryptPkcs7Base64(got, conn.AesKey, conn.AesIV)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(plain, payload) {
				t.Fatal("密文解密后应还原完整消息")
			}
		})
	}
}

func TestEncryptMessagePayloadConcurrentSharedInput(t *testing.T) {
	t.Parallel()
	const workers, repeats = 32, 32
	backing := bytes.Repeat([]byte{0xa5}, 128)
	payload := backing[:17]
	copy(payload, "shared-message-01")
	before := bytes.Clone(backing)
	want := bytes.Clone(payload)
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// 各接收者使用不同密钥，但并行读取同一消息缓冲区。
			conn := &eventbus.Conn{
				AesKey: bytes.Repeat([]byte{byte(worker + 1)}, 16),
				AesIV:  bytes.Repeat([]byte{byte(worker + 33)}, 16),
			}
			<-start
			for i := 0; i < repeats; i++ {
				ciphertext, err := encryptMessagePayload(payload, conn)
				if err != nil {
					errs <- fmt.Errorf("接收者 %d 加密失败: %w", worker, err)
					return
				}
				plain, err := wkutil.AesDecryptPkcs7Base64(ciphertext, conn.AesKey, conn.AesIV)
				if err != nil {
					errs <- fmt.Errorf("接收者 %d 解密失败: %w", worker, err)
					return
				}
				if !bytes.Equal(plain, want) {
					errs <- fmt.Errorf("接收者 %d 解密消息与原文不一致", worker)
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if !bytes.Equal(backing, before) {
		t.Fatal("并发加密修改了共享消息底层数组或其预留容量")
	}
}

func pushTestConn(uid string, id int64, session string) *eventbus.Conn {
	return &eventbus.Conn{Uid: uid, NodeId: 1, ConnId: id, SessionId: session, Auth: true}
}

type pushSessionUsers struct {
	eventbus.IUser
	conns  []*eventbus.Conn
	writes []*eventbus.Event
}

func (u *pushSessionUsers) AuthedConnsByUid(uid string) []*eventbus.Conn {
	conns := make([]*eventbus.Conn, 0, len(u.conns))
	for _, conn := range u.conns {
		if conn.Uid == uid && conn.Auth {
			conns = append(conns, conn)
		}
	}
	return conns
}

func (u *pushSessionUsers) AddEvent(_ string, event *eventbus.Event) {
	u.writes = append(u.writes, event)
}

func (u *pushSessionUsers) Advance(string) {}
