package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/client"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/jsonrpc"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Test Helpers ---

// connectRawTCP connects to the server's TCP address and returns the connection.
func connectRawTCP(t testing.TB, addr string) net.Conn {
	conn, err := net.DialTimeout("tcp", addr, time.Second*3)
	require.NoError(t, err)
	require.NotNil(t, conn)
	return conn
}

// sendJSON sends an encoded JSON-RPC message over the connection.
func sendJSON(t testing.TB, conn net.Conn, msg interface{}) {
	jsonData, err := jsonrpc.Encode(msg)
	require.NoError(t, err)
	_, err = conn.Write(jsonData)
	require.NoError(t, err)
}

// readJSON decodes the next JSON-RPC message from the connection using json.Decoder.
// It handles potential timeouts.
func readJSON(t testing.TB, conn net.Conn, timeout time.Duration) (interface{}, jsonrpc.Probe) {
	err := conn.SetReadDeadline(time.Now().Add(timeout))
	require.NoError(t, err)

	decoder := json.NewDecoder(conn)
	msg, probe, err := jsonrpc.Decode(decoder)

	errDeadline := conn.SetReadDeadline(time.Time{})
	require.NoError(t, errDeadline)

	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			require.FailNow(t, "readJSON timed out waiting for message")
		}
		require.NoError(t, err, "Failed to decode JSON-RPC message")
	}

	require.NotNil(t, msg, "Decoded JSON-RPC message should not be nil")
	return msg, probe
}

// Helper copied/adapted from codec_test.go
func assertDecodedAs[T any](t *testing.T, decodedMsg interface{}) T {
	req := require.New(t)
	msg, ok := decodedMsg.(T)
	req.Truef(ok, "Decoded message type is not %T, but %T", *new(T), decodedMsg)
	return msg
}

// --- Original Test Cases ---

func TestServerStart(t *testing.T) {
	s := NewTestServer(t)
	s.opts.Mode = options.TestMode
	err := s.Start()
	assert.Nil(t, err)
	err = s.Stop()
	assert.Nil(t, err)
}

// 测试单节点发送消息
func TestSingleSendMessage(t *testing.T) {
	s := NewTestServer(t)
	s.opts.Mode = options.TestMode
	err := s.Start()
	assert.Nil(t, err)
	defer s.StopNoErr()

	s.MustWaitAllSlotsReady(time.Second * 10) // 等待服务准备好

	// new client 1
	cli1 := client.New(s.opts.External.TCPAddr, client.WithUID("test1"))
	err = cli1.Connect()
	assert.Nil(t, err)
	defer cli1.Close()

	// new client 2
	cli2 := client.New(s.opts.External.TCPAddr, client.WithUID("test2"))
	err = cli2.Connect()
	assert.Nil(t, err)
	defer cli2.Close()

	// cli2 recv
	recvDone := make(chan struct{}, 1)
	cli2.SetOnRecv(func(recv *wkproto.RecvPacket) error {
		assert.Equal(t, "hello", string(recv.Payload))
		recvDone <- struct{}{}
		return nil
	})

	// send message
	err = cli1.SendMessage(client.NewChannel("test2", 1), []byte("hello"))
	assert.Nil(t, err)

	select {
	case <-recvDone:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the recipient to receive the message")
	}
}

func TestClientCannotSendReservedWalletContent(t *testing.T) {
	s := NewTestServer(t)
	s.opts.Mode = options.TestMode
	require.NoError(t, s.Start())
	defer s.StopNoErr()
	s.MustWaitAllSlotsReady(10 * time.Second)

	clientCases := []struct {
		name      string
		addr      string
		uid       string
		noPersist bool
	}{
		{name: "encrypted tcp client", addr: s.opts.External.TCPAddr, uid: "reserved-wallet-tcp"},
		{name: "no persist client", addr: s.opts.External.TCPAddr, uid: "reserved-wallet-no-persist", noPersist: true},
	}
	for _, tt := range clientCases {
		t.Run(tt.name, func(t *testing.T) {
			cli := client.New(tt.addr, client.WithUID(tt.uid))
			ackCh := make(chan wkproto.ReasonCode, 1)
			cli.SetOnSendack(func(ack *wkproto.SendackPacket) {
				ackCh <- ack.ReasonCode
			})
			require.NoError(t, cli.Connect())
			defer cli.Close()

			require.NoError(t, cli.SendMessage(
				client.NewChannel("receiver", wkproto.ChannelTypePerson),
				[]byte(`{"type":9}`),
				client.SendOptionWithNoPersist(tt.noPersist),
				client.SendOptionWithClientMsgNo("blocked-"+tt.uid),
			))
			select {
			case reason := <-ackCh:
				require.Equal(t, wkproto.ReasonNotAllowSend, reason)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for reserved wallet message sendack")
			}
			if !tt.noPersist {
				fakeChannelID := options.GetFakeChannelIDWith(tt.uid, "receiver")
				require.Never(t, func() bool {
					message, err := service.Store.LoadMsgByClientMsgNo(
						fakeChannelID,
						wkproto.ChannelTypePerson,
						"blocked-"+tt.uid,
					)
					return err == nil && !wkdb.IsEmptyMessage(message)
				}, 300*time.Millisecond, 20*time.Millisecond, "rejected client wallet UI must not be persisted")
			}
		})
	}

	t.Run("websocket json rpc client", func(t *testing.T) {
		wsURL := strings.Replace(s.opts.External.WSAddr, "0.0.0.0", "127.0.0.1", 1)
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		require.NoError(t, err)
		defer conn.Close()

		writeJSON := func(message interface{}) {
			encoded, encodeErr := jsonrpc.Encode(message)
			require.NoError(t, encodeErr)
			require.NoError(t, conn.WriteMessage(websocket.BinaryMessage, encoded))
		}
		readJSON := func() interface{} {
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, encoded, readErr := conn.ReadMessage()
			require.NoError(t, readErr)
			decoded, _, decodeErr := jsonrpc.Decode(json.NewDecoder(bytes.NewReader(encoded)))
			require.NoError(t, decodeErr)
			return decoded
		}

		writeJSON(jsonrpc.ConnectRequest{
			BaseRequest: jsonrpc.BaseRequest{Method: jsonrpc.MethodConnect, ID: "reserved-ws-connect"},
			Params: jsonrpc.ConnectParams{
				Version: wkproto.LatestVersion, DeviceID: "reserved-ws-device", UID: "reserved-wallet-ws", Token: "token",
			},
		})
		_ = readJSON()
		writeJSON(jsonrpc.SendRequest{
			BaseRequest: jsonrpc.BaseRequest{Method: jsonrpc.MethodSend, ID: "reserved-ws-send"},
			Params: jsonrpc.SendParams{
				ChannelID: "receiver", ChannelType: int(wkproto.ChannelTypePerson), Payload: json.RawMessage(`{"type":9}`),
			},
		})
		response := assertDecodedAs[jsonrpc.GenericResponse](t, readJSON())
		var result jsonrpc.SendResult
		require.NoError(t, json.Unmarshal(response.Result, &result))
		require.Equal(t, jsonrpc.ReasonCodeEnum(wkproto.ReasonNotAllowSend), result.ReasonCode)
	})

	t.Run("json rpc client", func(t *testing.T) {
		conn := connectRawTCP(t, s.opts.External.TCPAddr)
		defer conn.Close()
		sendJSON(t, conn, jsonrpc.ConnectRequest{
			BaseRequest: jsonrpc.BaseRequest{Method: jsonrpc.MethodConnect, ID: "reserved-connect"},
			Params: jsonrpc.ConnectParams{
				Version: wkproto.LatestVersion, DeviceID: "reserved-device", UID: "reserved-wallet-json", Token: "token",
			},
		})
		_, _ = readJSON(t, conn, 5*time.Second)

		sendJSON(t, conn, jsonrpc.SendRequest{
			BaseRequest: jsonrpc.BaseRequest{Method: jsonrpc.MethodSend, ID: "reserved-send"},
			Params: jsonrpc.SendParams{
				ChannelID: "receiver", ChannelType: int(wkproto.ChannelTypePerson), Payload: json.RawMessage(`{"type":"10"}`),
			},
		})
		decoded, _ := readJSON(t, conn, 5*time.Second)
		response := assertDecodedAs[jsonrpc.GenericResponse](t, decoded)
		var result jsonrpc.SendResult
		require.NoError(t, json.Unmarshal(response.Result, &result))
		require.Equal(t, jsonrpc.ReasonCodeEnum(wkproto.ReasonNotAllowSend), result.ReasonCode)
	})
}

// --- New JSON-RPC Test Cases ---

// TestSingleJSONRPC_ConnectSendRecv tests basic connect, send, and recv using JSON-RPC.
func TestSingleJSONRPC_ConnectSendRecv(t *testing.T) {
	s := NewTestServer(t)
	s.opts.Mode = options.TestMode
	err := s.Start()
	assert.Nil(t, err)
	defer s.StopNoErr()

	s.MustWaitAllSlotsReady(time.Second * 10)

	var wg sync.WaitGroup
	var recvPayload []byte

	// --- Client 2 Setup (Receiver) ---
	conn2 := connectRawTCP(t, s.opts.External.TCPAddr)
	defer conn2.Close()

	// Send Connect Request for Client 2
	connectReq2 := jsonrpc.ConnectRequest{
		BaseRequest: jsonrpc.BaseRequest{
			Method: jsonrpc.MethodConnect,
			ID:     "conn-2",
		},
		Params: jsonrpc.ConnectParams{
			Version:  wkproto.LatestVersion,
			DeviceID: "device2",
			UID:      "test2",
			Token:    "token2",
		},
	}
	sendJSON(t, conn2, connectReq2)

	// Read Connect Response for Client 2
	respMsg2, _ := readJSON(t, conn2, time.Second*5)
	resp2 := assertDecodedAs[jsonrpc.GenericResponse](t, respMsg2)
	assert.Equal(t, "conn-2", resp2.ID)
	assert.Nil(t, resp2.Error)
	assert.NotNil(t, resp2.Result)

	// Start goroutine to wait for message on Client 2 connection
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Read Recv Notification for Client 2
		recvMsg, _ := readJSON(t, conn2, time.Second*10)
		recvNotif := assertDecodedAs[jsonrpc.RecvNotification](t, recvMsg)
		assert.Equal(t, jsonrpc.MethodRecv, recvNotif.Method)
		assert.NotNil(t, recvNotif.Params)

		// Extract payload for assertion later
		var payloadData struct {
			Data string `json:"data"`
		}
		err := json.Unmarshal(recvNotif.Params.Payload, &payloadData)
		require.NoError(t, err)
		recvPayload = []byte(payloadData.Data)
	}()

	// --- Client 1 Setup (Sender) ---
	conn1 := connectRawTCP(t, s.opts.External.TCPAddr)
	defer conn1.Close()

	// Send Connect Request for Client 1
	connectReq1 := jsonrpc.ConnectRequest{
		BaseRequest: jsonrpc.BaseRequest{
			Method: jsonrpc.MethodConnect,
			ID:     "conn-1",
		},
		Params: jsonrpc.ConnectParams{
			Version:  wkproto.LatestVersion,
			DeviceID: "device1",
			UID:      "test1",
			Token:    "token1",
		},
	}
	sendJSON(t, conn1, connectReq1)

	// Read Connect Response for Client 1
	respMsg1, _ := readJSON(t, conn1, time.Second*5)
	resp1 := assertDecodedAs[jsonrpc.GenericResponse](t, respMsg1)
	assert.Equal(t, "conn-1", resp1.ID)
	assert.Nil(t, resp1.Error)
	assert.NotNil(t, resp1.Result)

	// --- Send Message from Client 1 to Client 2 ---
	time.Sleep(100 * time.Millisecond) // Small delay to ensure receiver is likely ready

	sendReq := jsonrpc.SendRequest{
		BaseRequest: jsonrpc.BaseRequest{
			Method: jsonrpc.MethodSend,
			ID:     "send-1",
		},
		Params: jsonrpc.SendParams{
			ChannelID:   "test2",
			ChannelType: int(wkproto.ChannelTypePerson), // Cast to int
			Payload:     json.RawMessage(`{"data":"hello jsonrpc"}`),
		},
	}
	sendJSON(t, conn1, sendReq)

	// Read Send Response for Client 1
	sendRespMsg, _ := readJSON(t, conn1, time.Second*5)
	sendResp := assertDecodedAs[jsonrpc.GenericResponse](t, sendRespMsg)
	assert.Equal(t, "send-1", sendResp.ID)
	assert.Nil(t, sendResp.Error)
	assert.NotNil(t, sendResp.Result)

	// Wait for Client 2 to receive the message
	wg.Wait()

	assert.Equal(t, "hello jsonrpc", string(recvPayload))
}

// --- Rest of Existing Test Cases ---

func TestClusterSendMessage(t *testing.T) {
	nodes := startProcessCluster(t, 2, 1)
	sender := nodes[0].connect("test1")
	receiver := nodes[1].connect("test2")
	messages := receiveProcessMessage(t, receiver)
	require.NoError(t, sender.SendMessage(client.NewChannel("test2", 1), []byte("hello")))
	requireProcessMessage(t, messages, "hello")
}

func TestClusterSlotMigrate(t *testing.T) {
	testProcessSlotMigrate(t, 1)
}

// 两副本场景仍验证实际领导权迁移，不以空实现返回 nil 作为成功。
func TestClusterSlotMigrateForFollowToLeader(t *testing.T) {
	testProcessSlotMigrate(t, 2)
}

func testProcessSlotMigrate(t *testing.T, replicas int) {
	t.Helper()
	nodes := startProcessCluster(t, 2, replicas)
	var slot *types.Slot
	for _, candidate := range nodes[0].clusterConfig().Slots {
		if candidate.Leader == nodes[0].config.ID {
			slot = candidate
			break
		}
	}
	require.NotNil(t, slot)
	processLeader(t, nodes).call(clusterProcessRequest{
		Op: "migrate", SlotID: slot.Id, From: nodes[0].config.ID, To: nodes[1].config.ID,
	})
	nodes[0].waitConfig(func(cfg *types.Config) bool {
		for _, candidate := range cfg.Slots {
			if candidate.Id == slot.Id {
				return candidate.Leader == nodes[1].config.ID
			}
		}
		return false
	})
}

func TestClusterNodeJoin(t *testing.T) {
	nodes := startProcessCluster(t, 2, 1)
	require.Len(t, nodes[0].clusterConfig().Nodes, 2)
	joining := newClusterProcess(t, 1005, 1)
	joining.config.Seed = fmt.Sprintf("%d@%s", nodes[0].config.ID, nodes[0].config.ClusterAddr)
	joining.start()
	// 先确认控制面接纳节点，再独立等待槽迁移；二者是串行的异步阶段。
	joining.waitConfig(func(cfg *types.Config) bool {
		if len(cfg.Nodes) != 3 || wkutil.ArrayContainsUint64(cfg.Learners, joining.config.ID) {
			return false
		}
		for _, node := range cfg.Nodes {
			if node.Id == joining.config.ID {
				return node.Status == types.NodeStatus_NodeStatusJoining || node.Status == types.NodeStatus_NodeStatusJoined
			}
		}
		return false
	})
	joining.waitConfig(func(cfg *types.Config) bool {
		if len(cfg.Nodes) != 3 {
			return false
		}
		hasReplica := false
		for _, slot := range cfg.Slots {
			if wkutil.ArrayContainsUint64(slot.Learners, joining.config.ID) {
				return false
			}
			hasReplica = hasReplica || wkutil.ArrayContainsUint64(slot.Replicas, joining.config.ID)
		}
		return hasReplica
	})
}

// func TestClusterChannelMigrate(t *testing.T) {
// 	s1, s2 := NewTestClusterServerTwoNode(t, options.WithClusterChannelReplicaCount(1), options.WithClusterSlotReplicaCount(2))
// 	err := s1.Start()
// 	assert.Nil(t, err)

// 	err = s2.Start()
// 	assert.Nil(t, err)

// 	defer s1.StopNoErr()
// 	defer s2.StopNoErr()

// 	MustWaitClusterReady(s1, s2)

// 	// new client 1
// 	cli1 := client.New(s1.opts.External.TCPAddr, client.WithUID("test1"))
// 	err = cli1.Connect()
// 	assert.Nil(t, err)

// 	// new client 2
// 	cli2 := client.New(s2.opts.External.TCPAddr, client.WithUID("test2"))
// 	err = cli2.Connect()
// 	assert.Nil(t, err)

// 	// send message to test2
// 	err = cli1.SendMessage(client.NewChannel("test2", 1), []byte("hello"))
// 	assert.Nil(t, err)

// 	var wait sync.WaitGroup
// 	wait.Add(1)

// 	// cli2 recv
// 	cli2.SetOnRecv(func(recv *wkproto.RecvPacket) error {
// 		assert.Equal(t, "hello", string(recv.Payload))
// 		wait.Done()
// 		return nil
// 	})

// 	wait.Wait()

// 	cfg, err := s1.store.DB().GetChannelClusterConfig("test1@test2", 1)
// 	assert.Nil(t, err)
// 	assert.Equal(t, 1, len(cfg.Replicas))

// 	// 迁移到另外一个节点

// 	var migrateTo uint64
// 	if cfg.Replicas[0] == s1.opts.Cluster.NodeId {
// 		migrateTo = s2.opts.Cluster.NodeId
// 	} else {
// 		migrateTo = s1.opts.Cluster.NodeId
// 	}
// 	cfg.MigrateFrom = cfg.Replicas[0]
// 	cfg.MigrateTo = migrateTo
// 	cfg.Learners = append(cfg.Learners, migrateTo)

// 	err = s1.clusterServer.ProposeChannelClusterConfig(cfg)
// 	assert.Nil(t, err)

// 	s1.clusterServer.UpdateChannelClusterConfig(cfg)
// 	s2.clusterServer.UpdateChannelClusterConfig(cfg)

// 	time.Sleep(time.Second * 1)

// }

func TestClusterChannelElection(t *testing.T) {
	nodes := startProcessCluster(t, 3, 3)
	sender := nodes[0].connect("test1")
	receiver := nodes[1].connect("test2")
	messages := receiveProcessMessage(t, receiver)
	require.NoError(t, sender.SendMessage(client.NewChannel("test2", 1), []byte("hello")))
	requireProcessMessage(t, messages, "hello")

	channelID := options.GetFakeChannelIDWith("test1", "test2")
	leaderID := nodes[0].call(clusterProcessRequest{
		Op: "channel-leader", ChannelID: channelID, ChannelType: 1,
	}).NodeID
	var stopped, survivor *clusterProcess
	for _, node := range nodes {
		if node.config.ID == leaderID {
			stopped = node
		} else {
			survivor = node
		}
	}
	require.NotNil(t, stopped)
	require.NotNil(t, survivor)
	stopped.stop()
	for _, node := range nodes {
		if node != stopped {
			node.waitFailover(leaderID)
		}
	}

	sender.Close()
	receiver.Close()
	sender = survivor.connect("test1")
	receiver = survivor.connect("test2")
	messages = receiveProcessMessage(t, receiver)
	require.NoError(t, sender.SendMessage(client.NewChannel("test2", 1), []byte("after-election")))
	requireProcessMessage(t, messages, "after-election")
}

// 群故障转移必须先建立订阅者，再验证故障前后真实收发。
func TestClusterFailover(t *testing.T) {
	nodes := startProcessCluster(t, 3, 3)
	sender := nodes[0].connect("u1")
	receiver2 := nodes[1].connect("u2")
	receiver3 := nodes[2].connect("u3")
	payload := bytes.NewBufferString(`{"channel_id":"g1","channel_type":2,"subscribers":["u1","u2","u3"]}`)
	httpClient := &http.Client{Timeout: 5 * time.Second}
	resp, err := httpClient.Post("http://"+nodes[0].config.HTTPAddr+"/channel/subscriber_add", "application/json", payload)
	require.NoError(t, err)
	defer resp.Body.Close()
	// 保留失败响应，区分群初始化错误和故障切换后的收发失败。
	responseBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "%s", responseBody)

	messages2 := receiveProcessMessage(t, receiver2)
	messages3 := receiveProcessMessage(t, receiver3)
	// 失败时保留用户路由与连接副本证据，不改变消息发送或原有等待预算。
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for _, node := range nodes {
			select {
			case <-node.exited:
				continue
			default:
			}
			for _, uid := range []string{"u1", "u2", "u3"} {
				state := node.call(clusterProcessRequest{Op: "user-state", UID: uid})
				t.Logf("节点 %d 用户 %s 当前领导 %d，认证连接数量 %d", node.config.ID, uid, state.NodeID, state.ConnCount)
			}
			messages := node.call(clusterProcessRequest{Op: "message-state", ChannelID: "g1", ChannelType: wkproto.ChannelTypeGroup})
			t.Logf("节点 %d 已保存的测试消息 %q", node.config.ID, messages.Payloads)
		}
	})
	channel := client.NewChannel("g1", wkproto.ChannelTypeGroup)
	require.NoError(t, sender.SendMessage(channel, []byte("hello")))
	requireProcessMessage(t, messages2, "hello")
	requireProcessMessage(t, messages3, "hello")

	nodes[2].stop()
	nodes[0].waitFailover(nodes[2].config.ID)
	nodes[1].waitFailover(nodes[2].config.ID)
	receiver3.Close()
	receiver3 = nodes[1].connect("u3")
	messages3 = receiveProcessMessage(t, receiver3)
	require.NoError(t, sender.SendMessage(channel, []byte("hello2")))
	requireProcessMessage(t, messages2, "hello2")
	requireProcessMessage(t, messages3, "hello2")
}

func TestClusterSaveClusterConfig(t *testing.T) {
	nodes := startProcessCluster(t, 3, 3)
	now := time.Now()
	cfg := wkdb.ChannelClusterConfig{
		ChannelId: "test1@test2", ChannelType: 1, ReplicaMaxCount: 3,
		Replicas: []uint64{nodes[0].config.ID, nodes[1].config.ID, nodes[2].config.ID},
		LeaderId: nodes[0].config.ID, Learners: []uint64{}, CreatedAt: &now, UpdatedAt: &now,
	}
	nodes[0].call(clusterProcessRequest{Op: "save-channel", Config: cfg})
	saved := nodes[0].call(clusterProcessRequest{
		Op: "load-channel", ChannelID: cfg.ChannelId, ChannelType: cfg.ChannelType,
	}).Channel
	require.Equal(t, cfg.ChannelId, saved.ChannelId)
	require.Equal(t, cfg.LeaderId, saved.LeaderId)
	require.Equal(t, cfg.Replicas, saved.Replicas)
}
