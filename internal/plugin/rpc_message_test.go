package plugin

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/wkrpc"
	"github.com/WuKongIM/wkrpc/proto"
	"github.com/panjf2000/gnet/v2"
	"github.com/spf13/viper"
)

type textMessageRPCConn struct {
	gnet.Conn
	responses [][]byte
}

func (*textMessageRPCConn) Context() any { return nil }

func (c *textMessageRPCConn) AsyncWrite(data []byte, _ gnet.AsyncCallback) error {
	c.responses = append(c.responses, append([]byte(nil), data...))
	return nil
}

type textMessageRPCChannel struct{ events []*eventbus.Event }

func (c *textMessageRPCChannel) AddEvent(_ string, _ uint8, event *eventbus.Event) {
	c.events = append(c.events, event)
}

func (*textMessageRPCChannel) Advance(string, uint8) {}

func TestPluginMessageSendRejectsOverlongTextBeforeEnqueueAndSuccess(t *testing.T) {
	previousOptions, previousChannel := options.G, eventbus.Channel
	options.G = options.New()
	config := viper.New()
	config.Set("rootDir", t.TempDir())
	config.Set("jwt.secret", "unit-test-only")
	options.G.ConfigureWithViper(config)
	t.Cleanup(func() { options.G, eventbus.Channel = previousOptions, previousChannel })
	for _, count := range []int{5000, 5001} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			conn, channel := &textMessageRPCConn{}, &textMessageRPCChannel{}
			eventbus.RegisterChannel(channel)
			payload, _ := json.Marshal(map[string]any{"type": 1, "content": strings.Repeat("😀", count)})
			rpc := &rpc{Log: wklog.NewWKLog("text-rpc-test")}
			rpc.messageSendRequest(wkrpc.NewContext(conn), &pluginproto.SendReq{
				FromUid: "sender", ChannelId: "group", ChannelType: 2, ClientMsgNo: "client", Payload: payload,
			})
			if len(conn.responses) != 1 {
				t.Fatalf("RPC回复次数 = %d，应该为一次", len(conn.responses))
			}
			headerSize := proto.MagicNumberStartLength + proto.MsgTypeLength + proto.MsgContentLength
			response := &proto.Response{}
			if err := response.Unmarshal(conn.responses[0][headerSize:]); err != nil {
				t.Fatal(err)
			}
			if count == 5000 {
				if len(channel.events) != 1 || response.Status != proto.StatusOK {
					t.Fatalf("合法消息入队 %d 次，状态 = %v", len(channel.events), response.Status)
				}
			} else if len(channel.events) != 0 || response.Status != proto.StatusError {
				t.Fatalf("拒绝消息入队 %d 次，状态 = %v", len(channel.events), response.Status)
			}
		})
	}
}
