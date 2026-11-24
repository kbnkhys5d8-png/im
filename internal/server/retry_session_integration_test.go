//go:build integration

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/client"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

func TestRetrySessionLegacyTCPGroupDelivery(t *testing.T) {
	// 只使用临时数据目录和 loopback 随机端口，不连接已有 IM、API 或数据库。
	s, api := startConnectionFollowupTestServer(t, options.WithTokenAuthOn(true))
	require.True(t, options.G.TokenAuthOn)
	address := s.engine.TCPRealListenAddr().String()
	httpClient := &http.Client{Timeout: 5 * time.Second}
	post := func(t *testing.T, path string, body interface{}) {
		t.Helper()
		encoded, err := json.Marshal(body)
		require.NoError(t, err)
		response, err := httpClient.Post(api+path, "application/json", bytes.NewReader(encoded))
		require.NoError(t, err)
		defer response.Body.Close()
		result, err := io.ReadAll(io.LimitReader(response.Body, 8192))
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, response.StatusCode, string(result))
	}
	for _, version := range []uint8{3, 4, 5} {
		t.Run(fmt.Sprintf("protocol-%d", version), func(t *testing.T) {
			senderUID := fmt.Sprintf("retry-sender-%d", version)
			receiverUID := fmt.Sprintf("retry-receiver-%d", version)
			group := fmt.Sprintf("retry-group-%d", version)
			post(t, "/user/token", map[string]interface{}{
				"uid": senderUID, "token": "local-test-token", "device_flag": wkproto.APP, "device_level": wkproto.DeviceLevelMaster,
			})
			post(t, "/channel/subscriber_add", map[string]interface{}{
				"channel_id": group, "channel_type": wkproto.ChannelTypeGroup, "subscribers": []string{senderUID},
			})
			// 先生成旧群路由，再创建和连接新成员，覆盖“已在线后加入旧群”的路径。
			post(t, "/message/send", map[string]interface{}{
				"from_uid": senderUID, "channel_id": group, "channel_type": wkproto.ChannelTypeGroup,
				"payload": []byte(`{"type":1,"content":"seed-before-join"}`),
			})
			require.Eventually(t, func() bool { return service.TagManager.GetChannelTag(group, wkproto.ChannelTypeGroup) != "" }, 2*time.Second, 5*time.Millisecond)
			post(t, "/user/token", map[string]interface{}{
				"uid": receiverUID, "token": "local-test-token", "device_flag": wkproto.APP, "device_level": wkproto.DeviceLevelMaster,
			})
			seen := make(map[int64]struct{})
			previousSession := ""
			for phase := 0; phase < 2; phase++ {
				receiver := client.New(address, client.WithUID(receiverUID), client.WithToken("local-test-token"), client.WithProtoVersion(version))
				t.Cleanup(receiver.Close)
				received := make(chan *wkproto.RecvPacket, 32)
				receiver.SetOnRecv(func(packet *wkproto.RecvPacket) error { received <- packet; return nil })
				require.NoError(t, receiver.Connect())
				connections := eventbus.User.AuthedConnsByUid(receiverUID)
				require.Len(t, connections, 1)
				require.NotEmpty(t, connections[0].SessionId)
				require.NotEqual(t, previousSession, connections[0].SessionId)
				previousSession = connections[0].SessionId
				if phase == 0 {
					post(t, "/channel/subscriber_add", map[string]interface{}{
						"channel_id": group, "channel_type": wkproto.ChannelTypeGroup, "subscribers": []string{receiverUID},
					})
				}
				expected := make(map[string]struct{}, 20)
				for i := 0; i < 20; i++ {
					content := fmt.Sprintf("phase-%d-message-%d", phase, i)
					expected[content] = struct{}{}
					payload := []byte(fmt.Sprintf(`{"type":1,"content":%q}`, content))
					// 后端接口驱动下行，接收端仍通过真实 TCP 解密并按旧协议确认。
					post(t, "/message/send", map[string]interface{}{
						"from_uid": senderUID, "channel_id": group, "channel_type": wkproto.ChannelTypeGroup,
						"payload": payload, "header": map[string]int{"red_dot": 1},
					})
				}
				for i := 0; i < 20; i++ {
					select {
					case packet := <-received:
						require.Equal(t, group, packet.ChannelID)
						require.Positive(t, packet.MessageSeq)
						require.True(t, packet.RedDot)
						require.False(t, packet.NoPersist)
						_, duplicate := seen[packet.MessageID]
						require.False(t, duplicate)
						seen[packet.MessageID] = struct{}{}
						var body struct {
							Content string `json:"content"`
						}
						require.NoError(t, json.Unmarshal(packet.Payload, &body))
						_, present := expected[body.Content]
						require.True(t, present, "消息内容不能遗漏、重复或跨重连批次")
						delete(expected, body.Content)
					case <-time.After(5 * time.Second):
						t.Fatalf("group delivery missing protocol=%d phase=%d index=%d", version, phase, i)
					}
				}
				require.Empty(t, expected)
				// 客户端沿原协议自动 ACK；所有持久化下行都应从重试表退出。
				require.Eventually(t, func() bool { return service.RetryManager.RetryMessageCount() == 0 }, 2*time.Second, 5*time.Millisecond)
				receiver.Close()
				require.Eventually(t, func() bool { return len(eventbus.User.AuthedConnsByUid(receiverUID)) == 0 }, 2*time.Second, 5*time.Millisecond)
			}
			require.Len(t, seen, 40)
		})
	}
}
