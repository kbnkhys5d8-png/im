package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

type textSendPermissionDB struct{ wkdb.DB }

func (textSendPermissionDB) GetChannel(string, uint8) (wkdb.ChannelInfo, error) {
	return wkdb.ChannelInfo{}, nil
}

type textSendUser struct {
	eventbus.IUser
	events []*eventbus.Event
	conn   *eventbus.Conn
}

// 会话门禁前登记夹具连接，原正文和钱包断言继续验证完整发送路径。
func (u *textSendUser) ConnById(string, uint64, int64) *eventbus.Conn { return u.conn }

func (u *textSendUser) AddEvent(_ string, event *eventbus.Event) {
	u.events = append(u.events, event)
}

type textSendChannel struct{ events []*eventbus.Event }

func (c *textSendChannel) AddEvent(_ string, _ uint8, event *eventbus.Event) {
	c.events = append(c.events, event)
}

func (c *textSendChannel) Advance(string, uint8) {}

type textSendPluginManager struct {
	service.IPluginManager
	plugins []types.Plugin
}

func (m textSendPluginManager) Plugins(...types.PluginMethod) []types.Plugin { return m.plugins }

type textRewritePlugin struct {
	types.Plugin
	payload []byte
}

func (textRewritePlugin) GetNo() string { return "text-rewrite-test" }

func (p textRewritePlugin) Send(_ context.Context, packet *pluginproto.SendPacket) (*pluginproto.SendPacket, error) {
	packet.Payload = p.payload
	return packet, nil
}

func TestHandleOnSendValidatesDecryptedAndPluginFinalTextBeforeEnqueue(t *testing.T) {
	previousOptions, previousStore, previousPlugins := options.G, service.Store, service.PluginManager
	previousUser, previousChannel, previousTrace := eventbus.User, eventbus.Channel, trace.GlobalTrace
	options.G = options.New()
	service.Store = store.New(store.NewOptions(store.WithDB(textSendPermissionDB{})))
	trace.GlobalTrace = trace.New(context.Background(), trace.NewOptions())
	t.Cleanup(func() {
		options.G, service.Store, service.PluginManager = previousOptions, previousStore, previousPlugins
		eventbus.User, eventbus.Channel, trace.GlobalTrace = previousUser, previousChannel, previousTrace
	})
	tests := []struct {
		name         string
		count        int
		jsonRPC      bool
		rewriteCount int
		wantReject   bool
	}{
		{name: "TCP和WSS解密后超限", count: 5001, wantReject: true},
		{name: "JSONRPC超限", count: 5001, jsonRPC: true, wantReject: true},
		{name: "插件最终改写超限", count: 1, rewriteCount: 5001, wantReject: true},
		{name: "四字节字符5000允许", count: 5000},
		{name: "插件最终改写5000允许", count: 1, rewriteCount: 5000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			user, channel := &textSendUser{}, &textSendChannel{}
			eventbus.RegisterUser(user)
			eventbus.RegisterChannel(channel)
			service.PluginManager = textSendPluginManager{}
			payload, _ := json.Marshal(map[string]any{"type": 1, "content": strings.Repeat("😀", tt.count)})
			if tt.rewriteCount > 0 {
				replacement, _ := json.Marshal(map[string]any{"type": 1, "content": strings.Repeat("😀", tt.rewriteCount)})
				service.PluginManager = textSendPluginManager{plugins: []types.Plugin{textRewritePlugin{payload: replacement}}}
			}
			conn := &eventbus.Conn{Uid: "sender", IsJsonRpc: tt.jsonRPC, AesKey: []byte("0123456789abcdef"), AesIV: []byte("0123456789abcdef")}
			packet := &wkproto.SendPacket{ChannelID: "group", ChannelType: 2, ClientMsgNo: "client", ClientSeq: 7, Payload: payload}
			if !tt.jsonRPC {
				encrypted, err := wkutil.AesEncryptPkcs7Base64(payload, conn.AesKey, conn.AesIV)
				if err != nil {
					t.Fatal(err)
				}
				packet.Payload = encrypted
				sign, err := wkutil.AesEncryptPkcs7Base64([]byte(packet.VerityString()), conn.AesKey, conn.AesIV)
				if err != nil {
					t.Fatal(err)
				}
				packet.MsgKey = wkutil.MD5Bytes(sign)
			}
			h := &Handler{Log: wklog.NewWKLog("text-limit-test")}
			h.handleOnSend(&eventbus.Event{Conn: conn, Frame: packet, MessageId: 123, ReqId: "req"})
			if tt.wantReject {
				if len(channel.events) != 0 || len(user.events) != 1 {
					t.Fatalf("拒绝消息入队 %d 次，ACK %d 次", len(channel.events), len(user.events))
				}
				ack, ok := user.events[0].Frame.(*wkproto.SendackPacket)
				if !ok || ack.ReasonCode != wkproto.ReasonPayloadDecodeError || ack.ClientSeq != 7 || ack.ClientMsgNo != "client" {
					t.Fatalf("未复用原失败ACK合同: %s", fmt.Sprint(user.events[0].Frame))
				}
			} else if len(channel.events) != 1 || len(user.events) != 0 {
				t.Fatalf("合法消息入队 %d 次，错误ACK %d 次", len(channel.events), len(user.events))
			}
		})
	}
}

type walletRewriteOncePlugin struct {
	types.Plugin
	payload []byte
	calls   int
}

func (*walletRewriteOncePlugin) GetNo() string { return "wallet-rewrite-test" }

func (p *walletRewriteOncePlugin) Send(_ context.Context, packet *pluginproto.SendPacket) (*pluginproto.SendPacket, error) {
	if p.calls == 0 {
		packet.Payload = p.payload
	}
	p.calls++
	return packet, nil
}

func TestHandleOnSendRejectsForgedWalletCardsAndContinuesBatch(t *testing.T) {
	previousOptions, previousStore, previousPlugins := options.G, service.Store, service.PluginManager
	previousUser, previousChannel, previousTrace := eventbus.User, eventbus.Channel, trace.GlobalTrace
	options.G = options.New()
	service.Store = store.New(store.NewOptions(store.WithDB(textSendPermissionDB{})))
	trace.GlobalTrace = trace.New(context.Background(), trace.NewOptions())
	t.Cleanup(func() {
		options.G, service.Store, service.PluginManager = previousOptions, previousStore, previousPlugins
		eventbus.User, eventbus.Channel, trace.GlobalTrace = previousUser, previousChannel, previousTrace
	})

	tests := []struct {
		name        string
		payload     string
		replacement string
		noPersist   bool
	}{
		{name: "整数转账", payload: `{"type":10,"amt":480000,"tx_no":12345}`},
		{name: "小数转账", payload: `{"type":10.1,"amt":480000,"tx_no":12345}`},
		{name: "小数红包", payload: `{"type":9.1}`},
		{name: "字符串小数", payload: `{"type":"10.1"}`},
		{name: "精度绕过", payload: `{"type":1.00000000000000000001}`},
		{name: "科学计数转账", payload: `{"type":1e1}`},
		{name: "小数整数转账", payload: `{"type":10.0}`},
		{name: "字符串整数前缀", payload: `{"type":"10e-1"}`},
		{name: "超大整数转账别名", payload: `{"type":4294967306}`},
		{name: "非持久消息也禁止伪造", payload: `{"type":10.1,"amt":480000}`, noPersist: true},
		{name: "转义字符串转账", payload: `{"type":"\u0031\u0030"}`},
		{name: "歧义类型", payload: `{"type":1,"type":2}`},
		{name: "缺少类型", payload: `{"content":"missing type"}`},
		{name: "插件改写整数转账", payload: `{"type":1,"content":"hello"}`, replacement: `{"type":10,"amt":480000,"tx_no":12345}`},
		{name: "插件改写小数转账", payload: `{"type":1,"content":"hello"}`, replacement: `{"type":10.1,"amt":480000,"tx_no":12345}`},
	}
	for _, tt := range tests {
		for _, jsonRPC := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/JSONRPC=%v", tt.name, jsonRPC), func(t *testing.T) {
				user, channel := &textSendUser{}, &textSendChannel{}
				eventbus.RegisterUser(user)
				eventbus.RegisterChannel(channel)
				service.PluginManager = textSendPluginManager{}
				if tt.replacement != "" {
					service.PluginManager = textSendPluginManager{plugins: []types.Plugin{&walletRewriteOncePlugin{payload: []byte(tt.replacement)}}}
				}
				// 客户端即使伪装系统设备和钱包消息编号，也不能取得服务端发卡权限。
				conn := &eventbus.Conn{Uid: "sender", DeviceId: options.G.SystemDeviceId, IsJsonRpc: jsonRPC, AesKey: []byte("0123456789abcdef"), AesIV: []byte("0123456789abcdef")}
				user.conn = conn
				packet := &wkproto.SendPacket{
					Framer:    wkproto.Framer{NoPersist: tt.noPersist},
					ChannelID: "peer", ChannelType: wkproto.ChannelTypePerson,
					ClientMsgNo: "wallet-transfer-12345", ClientSeq: 7, Payload: []byte(tt.payload),
				}
				if !jsonRPC {
					packet.Payload = encryptWalletTestPacket(t, packet, conn)
				}
				next := &wkproto.SendPacket{ChannelID: "peer", ChannelType: wkproto.ChannelTypePerson, ClientMsgNo: "ordinary", ClientSeq: 8, Payload: []byte(`{"type":1,"content":"hello"}`)}
				if !jsonRPC {
					next.Payload = encryptWalletTestPacket(t, next, conn)
				}
				h := &Handler{Log: wklog.NewWKLog("wallet-card-test")}
				// 两条消息放在同一次事件消费中，证明拒绝第一条不会中止整批。
				h.onSend(&eventbus.UserContext{Events: []*eventbus.Event{
					{Conn: conn, Frame: packet, MessageId: 123, ReqId: "reject"},
					{Conn: conn, Frame: next, MessageId: 124, ReqId: "accept"},
				}})
				if len(channel.events) != 1 || len(user.events) != 1 || channel.events[0].Frame != next {
					t.Fatalf("应该只投递后续正常消息，实际入队 %d 次，失败 ACK %d 次", len(channel.events), len(user.events))
				}
				ack, ok := user.events[0].Frame.(*wkproto.SendackPacket)
				if !ok || ack.ReasonCode != wkproto.ReasonNotAllowSend || ack.ClientSeq != 7 || ack.ClientMsgNo != "wallet-transfer-12345" {
					t.Fatalf("失败 ACK 未保留原消息身份: %v", user.events[0].Frame)
				}
			})
		}
	}
}

func encryptWalletTestPacket(t *testing.T, packet *wkproto.SendPacket, conn *eventbus.Conn) []byte {
	t.Helper()
	encrypted, err := wkutil.AesEncryptPkcs7Base64(packet.Payload, conn.AesKey, conn.AesIV)
	if err != nil {
		t.Fatal(err)
	}
	packet.Payload = encrypted
	sign, err := wkutil.AesEncryptPkcs7Base64([]byte(packet.VerityString()), conn.AesKey, conn.AesIV)
	if err != nil {
		t.Fatal(err)
	}
	packet.MsgKey = wkutil.MD5Bytes(sign)
	return encrypted
}

func TestResolveSendChannelIDRejectsRaftKeyDelimiter(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		channelType uint8
		uid         string
	}{
		{name: "customer service raw", raw: "a&b", channelType: wkproto.ChannelTypeCustomerService, uid: "user"},
		{name: "person raw", raw: "peer&x", channelType: wkproto.ChannelTypePerson, uid: "user"},
		{name: "agent raw", raw: "agent&x", channelType: wkproto.ChannelTypeAgent, uid: "user"},
		{name: "person derived", raw: "peer", channelType: wkproto.ChannelTypePerson, uid: "user&x"},
		{name: "agent derived", raw: "agent", channelType: wkproto.ChannelTypeAgent, uid: "user&x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := resolveSendChannelID(tt.raw, tt.channelType, tt.uid); ok {
				t.Fatal("channel identity containing raft key delimiter must be rejected")
			}
		})
	}
}

func TestResolveSendChannelIDPreservesValidChannels(t *testing.T) {
	tests := []struct {
		name        string
		raw         string
		channelType uint8
		uid         string
		want        string
	}{
		{
			name:        "customer service",
			raw:         "support",
			channelType: wkproto.ChannelTypeCustomerService,
			uid:         "user",
			want:        "support",
		},
		{
			name:        "person",
			raw:         "peer",
			channelType: wkproto.ChannelTypePerson,
			uid:         "user",
			want:        options.GetFakeChannelIDWith("peer", "user"),
		},
		{
			name:        "agent",
			raw:         "agent",
			channelType: wkproto.ChannelTypeAgent,
			uid:         "user",
			want:        options.GetAgentChannelIDWith("user", "agent"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := resolveSendChannelID(tt.raw, tt.channelType, tt.uid)
			if !ok {
				t.Fatal("valid channel identity was rejected")
			}
			if got != tt.want {
				t.Fatalf("resolved channel ID = %q, want %q", got, tt.want)
			}
		})
	}
}
