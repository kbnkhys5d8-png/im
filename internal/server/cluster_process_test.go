package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/client"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/stretchr/testify/require"
)

const clusterProcessEnv = "WK_CLUSTER_TEST_PROCESS"

type clusterProcessConfig struct {
	ID              uint64
	RootDir         string
	HTTPAddr        string
	TCPAddr         string
	WSAddr          string
	ClusterAddr     string
	SocketPath      string
	Nodes           []*options.Node
	Seed            string
	SlotReplicas    int
	ChannelReplicas int
}

type clusterProcessRequest struct {
	Op          string
	SlotID      uint32
	From        uint64
	To          uint64
	ChannelID   string
	ChannelType uint8
	Config      wkdb.ChannelClusterConfig
	UID         string
	ConnID      int64
	SessionID   string
}

type clusterProcessResponse struct {
	Error     string
	PID       int
	NodeID    uint64
	Config    *types.Config
	Channel   wkdb.ChannelClusterConfig
	ConnCount int
	Payloads  []string
	ConnID    int64
	SessionID string
}

// 仅测试中拦住指定用户的成功握手回执，不修改真实集群的其他调用。
type clusterProcessConnackGate struct {
	icluster.ICluster
	mu     sync.Mutex
	active *clusterProcessConnackPause
}

type clusterProcessConnackPause struct {
	uid         string
	arrived     chan struct{}
	releaseOnce sync.Once
	releaseErr  error
	released    bool
	captured    bool
	message     *proto.Message
	owner       uint64
	connID      int64
	sessionID   string
}

func (g *clusterProcessConnackGate) arm(uid string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if uid == "" || g.active != nil {
		return fmt.Errorf("握手门控用户为空或已经设置")
	}
	g.active = &clusterProcessConnackPause{uid: uid, arrived: make(chan struct{})}
	return nil
}

func (g *clusterProcessConnackGate) release() error {
	g.mu.Lock()
	pause := g.active
	g.mu.Unlock()
	if pause == nil {
		return nil
	}
	pause.releaseOnce.Do(func() {
		g.mu.Lock()
		pause.released = true
		message, owner := pause.message, pause.owner
		g.mu.Unlock()
		if message != nil {
			// 实际发送只执行一次，重复释放也返回同一结果，不能吞掉投递失败。
			pause.releaseErr = g.ICluster.Send(owner, message)
		}
	})
	return pause.releaseErr
}

func (g *clusterProcessConnackGate) wait() (clusterProcessResponse, error) {
	g.mu.Lock()
	pause := g.active
	g.mu.Unlock()
	if pause == nil {
		return clusterProcessResponse{}, fmt.Errorf("握手门控尚未设置")
	}
	select {
	case <-pause.arrived:
		g.mu.Lock()
		defer g.mu.Unlock()
		return clusterProcessResponse{NodeID: pause.owner, ConnID: pause.connID, SessionID: pause.sessionID}, nil
	case <-time.After(10 * time.Second):
		return clusterProcessResponse{}, fmt.Errorf("未捕获指定用户的握手回执")
	}
}

func (g *clusterProcessConnackGate) Send(nodeID uint64, message *proto.Message) error {
	// 2001 是现有集群用户事件包；测试只解码观察，不重写或伪造事件。
	if message.MsgType != 2001 {
		return g.ICluster.Send(nodeID, message)
	}
	g.mu.Lock()
	pause := g.active
	armed := pause != nil && !pause.captured && !pause.released
	g.mu.Unlock()
	if !armed {
		return g.ICluster.Send(nodeID, message)
	}
	decoder := wkproto.NewDecoder(message.Content)
	uid, err := decoder.String()
	if err != nil {
		return err
	}
	if _, err = decoder.Uint64(); err != nil {
		return err
	}
	if uid != pause.uid {
		return g.ICluster.Send(nodeID, message)
	}
	data, err := decoder.BinaryAll()
	if err != nil {
		return err
	}
	var events eventbus.EventBatch
	if err = events.Decode(data); err != nil {
		return err
	}
	for _, event := range events {
		ack, ok := event.Frame.(*wkproto.ConnackPacket)
		if event.Type != eventbus.EventConnack || !ok || ack.ReasonCode != wkproto.ReasonSuccess || event.Conn == nil {
			continue
		}
		g.mu.Lock()
		if pause.captured || pause.released {
			g.mu.Unlock()
			break
		}
		pause.captured = true
		pause.owner, pause.connID, pause.sessionID = nodeID, event.Conn.ConnId, event.Conn.SessionId
		// 模拟已接纳发送但网络仍在途；不阻塞旧领导事件结束及后续真实恢复。
		copyMessage := *message
		copyMessage.Content = append([]byte(nil), message.Content...)
		pause.message = &copyMessage
		close(pause.arrived)
		g.mu.Unlock()
		return nil
	}
	return g.ICluster.Send(nodeID, message)
}

// 这里只为现有默认集群测试隔离进程级单例，不给生产程序增加控制接口。
type clusterProcess struct {
	t        *testing.T
	config   clusterProcessConfig
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	replies  chan clusterProcessResponse
	exited   chan struct{}
	waitErr  error
	logPath  string
	stopOnce sync.Once
}

// TestClusterProcessNode 仅由父测试启动；每个子进程只创建一个 Server。
func TestClusterProcessNode(t *testing.T) {
	raw := os.Getenv(clusterProcessEnv)
	if raw == "" {
		t.Skip("仅用于集群测试子进程")
	}
	var cfg clusterProcessConfig
	require.NoError(t, json.Unmarshal([]byte(raw), &cfg))
	out := os.NewFile(3, "cluster-test-response")
	require.NotNil(t, out)
	defer out.Close()
	encoder := json.NewEncoder(out)
	s := NewTestServer(t,
		options.WithRootDir(cfg.RootDir),
		options.WithMode(options.TestMode),
		options.WithDemoOn(false),
		options.WithManagerOn(false),
		options.WithHTTPAddr(cfg.HTTPAddr),
		options.WithAddr("tcp://"+cfg.TCPAddr),
		options.WithWSAddr("ws://"+cfg.WSAddr),
		options.WithClusterAddr("tcp://"+cfg.ClusterAddr),
		options.WithClusterServerAddr(cfg.ClusterAddr),
		options.WithClusterAPIURL("http://"+cfg.HTTPAddr),
		options.WithClusterNodeId(cfg.ID),
		options.WithClusterInitNodes(cfg.Nodes),
		options.WithClusterSeed(cfg.Seed),
		options.WithClusterSlotReplicaCount(cfg.SlotReplicas),
		options.WithClusterChannelReplicaCount(cfg.ChannelReplicas),
		options.WithClusterPongMaxTick(10),
		options.WithClusterTickInterval(50*time.Millisecond),
		func(opts *options.Options) {
			opts.Plugin.SocketPath = cfg.SocketPath
		},
	)
	// 在启动任何后台协程前安装委托，避免测试运行中替换进程级单例。
	connackGate := &clusterProcessConnackGate{ICluster: service.Cluster}
	service.Cluster = connackGate
	require.NoError(t, s.Start())
	defer func() { require.NoError(t, s.Stop()) }()
	defer func() { require.NoError(t, connackGate.release()) }()
	require.NoError(t, encoder.Encode(clusterProcessResponse{PID: os.Getpid(), NodeID: cfg.ID}))
	decoder := json.NewDecoder(os.Stdin)
	for {
		var req clusterProcessRequest
		if err := decoder.Decode(&req); err != nil {
			if err == io.EOF {
				return
			}
			t.Fatal(err)
		}
		resp := clusterProcessResponse{}
		var err error
		switch req.Op {
		case "ready":
			// 单副本槽分散在不同节点，只等待当前节点实际负责的槽就绪。
			s.clusterServer.MustWaitAllSlotsReady(10 * time.Second)
		case "config":
			resp.Config = s.GetClusterConfig()
			resp.NodeID = s.clusterServer.LeaderId()
		case "migrate":
			// 旧 Server.MigrateSlot 是空壳，测试实际使用的集群配置提案路径。
			err = s.clusterServer.GetConfigServer().ProposeMigrateSlot(req.SlotID, req.From, req.To)
		case "channel-leader":
			var node *types.Node
			node, err = s.clusterServer.LeaderOfChannelForRead(req.ChannelID, req.ChannelType)
			if err == nil && node != nil {
				resp.NodeID = node.Id
			}
		case "save-channel":
			_, err = s.clusterServer.GetStore().SaveChannelClusterConfig(req.Config)
		case "load-channel":
			resp.Channel, err = s.clusterServer.LoadOnlyChannelClusterConfig(req.ChannelID, req.ChannelType)
		case "user-state":
			// 仅读取连接数量与当前路由，定位换主后接入连接是否仍在。
			resp.NodeID = s.clusterServer.SlotLeaderId(s.clusterServer.GetSlotId(req.UID))
			resp.ConnCount = len(eventbus.User.AuthedConnsByUid(req.UID))
		case "arm-connack":
			err = connackGate.arm(req.UID)
		case "wait-connack":
			resp, err = connackGate.wait()
		case "release-connack":
			err = connackGate.release()
		case "hold-connack-socket":
			// 只延长被测试门控的那个未认证 socket，生产初始 4 秒超时保持不变。
			realConn := service.ConnManager.GetConn(req.ConnID)
			if realConn == nil || realConn.IsClosed() {
				err = fmt.Errorf("被门控连接已经关闭")
				break
			}
			conn, ok := realConn.Context().(*eventbus.Conn)
			if !ok || conn == nil || conn.Uid != req.UID || conn.SessionId != req.SessionID || conn.Auth {
				err = fmt.Errorf("被门控连接的身份或未认证状态不匹配")
				break
			}
			realConn.SetMaxIdle(30 * time.Second)
		case "ensure-recovery":
			// 调用真实恢复流程，确认新领导先拿到空目录或只有另一设备的完整快照。
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = service.ConnRecovery.Ensure(ctx, req.UID)
			cancel()
			resp.ConnCount = len(eventbus.User.AuthedConnsByUid(req.UID))
		case "message-state":
			// 只读取本节点的测试消息，区分未保存与已保存但未投递。
			var messages []wkdb.Message
			messages, err = s.clusterServer.GetStore().DB().LoadLastMsgs(req.ChannelID, req.ChannelType, 2)
			for _, message := range messages {
				resp.Payloads = append(resp.Payloads, string(message.Payload))
			}
		default:
			err = fmt.Errorf("未知测试操作 %q", req.Op)
		}
		if err != nil {
			resp.Error = err.Error()
		}
		require.NoError(t, encoder.Encode(resp))
	}
}

func newClusterProcess(t *testing.T, id uint64, replicas int) *clusterProcess {
	t.Helper()
	// 长测试名会让 t.TempDir 超出 Unix socket 路径长度限制，单独使用短目录。
	socketDir, err := os.MkdirTemp("", "wkct-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(socketDir)) })
	addr := func() string { return fmt.Sprintf("127.0.0.1:%d", getFreePort(t)) }
	n := &clusterProcess{
		t: t,
		config: clusterProcessConfig{
			ID: id, RootDir: t.TempDir(), HTTPAddr: addr(), TCPAddr: addr(),
			WSAddr: addr(), ClusterAddr: addr(), SlotReplicas: replicas, ChannelReplicas: replicas,
			SocketPath: filepath.Join(socketDir, "wk.sock"),
		},
		replies: make(chan clusterProcessResponse),
		exited:  make(chan struct{}),
	}
	t.Cleanup(n.stop)
	return n
}

func startProcessCluster(t *testing.T, count, replicas int) []*clusterProcess {
	t.Helper()
	nodes := make([]*clusterProcess, 0, count)
	addresses := make([]*options.Node, 0, count)
	for i := 0; i < count; i++ {
		n := newClusterProcess(t, uint64(1001+i), replicas)
		nodes = append(nodes, n)
		addresses = append(addresses, &options.Node{Id: n.config.ID, ServerAddr: n.config.ClusterAddr})
	}
	for _, n := range nodes {
		n.config.Nodes = addresses
		n.start()
	}
	for _, n := range nodes {
		n.call(clusterProcessRequest{Op: "ready"})
	}
	for _, n := range nodes {
		// 槽就绪不代表节点 API 地址已复制，转发请求前必须确认地址可用。
		n.waitConfig(func(cfg *types.Config) bool {
			for _, expected := range nodes {
				found := false
				for _, actual := range cfg.Nodes {
					if actual.Id == expected.config.ID && actual.ApiServerAddr == "http://"+expected.config.HTTPAddr {
						found = true
						break
					}
				}
				if !found {
					return false
				}
			}
			return true
		})
	}
	return nodes
}

func (n *clusterProcess) start() {
	n.t.Helper()
	raw, err := json.Marshal(n.config)
	require.NoError(n.t, err)
	reader, writer, err := os.Pipe()
	require.NoError(n.t, err)
	defer writer.Close()
	readerTransferred := false
	defer func() {
		if !readerTransferred {
			reader.Close()
		}
	}()
	n.logPath = filepath.Join(n.config.RootDir, "process.log")
	log, err := os.Create(n.logPath)
	require.NoError(n.t, err)
	defer log.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestClusterProcessNode$", "-test.timeout=90s")
	cmd.Env = append(os.Environ(), clusterProcessEnv+"="+string(raw), "TMPDIR="+n.config.RootDir)
	cmd.ExtraFiles = []*os.File{writer}
	cmd.Stdout, cmd.Stderr = log, log
	stdin, err := cmd.StdinPipe()
	require.NoError(n.t, err)
	if err := cmd.Start(); err != nil {
		stdin.Close()
		n.t.Fatal(err)
	}
	// 完整启动后才交给清理函数，失败路径不会操作空句柄。
	n.cmd, n.stdin = cmd, stdin
	go func() {
		n.waitErr = n.cmd.Wait()
		close(n.exited)
	}()
	readerTransferred = true
	go func() {
		defer reader.Close()
		defer close(n.replies)
		decoder := json.NewDecoder(reader)
		for {
			var resp clusterProcessResponse
			if err := decoder.Decode(&resp); err != nil {
				return
			}
			select {
			case n.replies <- resp:
			case <-n.exited:
				return
			}
		}
	}()
	resp := n.receiveUntil(time.Now().Add(15 * time.Second))
	require.Equal(n.t, n.config.ID, resp.NodeID)
	require.Equal(n.t, n.cmd.Process.Pid, resp.PID)
	require.NotEqual(n.t, os.Getpid(), resp.PID, "节点必须独立于父测试进程")
}

func (n *clusterProcess) receiveUntil(deadline time.Time) clusterProcessResponse {
	n.t.Helper()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case resp, ok := <-n.replies:
		if ok {
			if resp.Error != "" {
				log, _ := os.ReadFile(n.logPath)
				n.t.Fatalf("节点 %d 测试命令失败：%s\n%s", n.config.ID, resp.Error, log)
			}
			return resp
		}
	case <-timer.C:
	}
	log, _ := os.ReadFile(n.logPath)
	n.t.Fatalf("节点 %d 未完成测试命令：\n%s", n.config.ID, log)
	return clusterProcessResponse{}
}

func (n *clusterProcess) call(req clusterProcessRequest) clusterProcessResponse {
	n.t.Helper()
	return n.callUntil(req, time.Now().Add(15*time.Second))
}

func (n *clusterProcess) callUntil(req clusterProcessRequest, deadline time.Time) clusterProcessResponse {
	n.t.Helper()
	require.NoError(n.t, json.NewEncoder(n.stdin).Encode(req))
	return n.receiveUntil(deadline)
}

func (n *clusterProcess) stop() {
	n.stopOnce.Do(func() {
		if n.cmd == nil {
			return
		}
		// 关闭控制输入触发正常停机；超时只终止本测试创建的确切子进程。
		n.stdin.Close()
		select {
		case <-n.exited:
		// 三节点清理会串行等待多个 5 秒下线提案，再关闭网络；仅给测试回收留足有界时间。
		case <-time.After(30 * time.Second):
			n.t.Errorf("节点 %d 停机超时", n.config.ID)
			// 超时已经判失败，先请求该子进程输出 Go 栈，再有界回收。
			if err := n.cmd.Process.Signal(syscall.SIGQUIT); err != nil {
				n.t.Logf("节点 %d 获取停机栈失败：%v", n.config.ID, err)
			}
			select {
			case <-n.exited:
			case <-time.After(2 * time.Second):
				n.cmd.Process.Kill()
				<-n.exited
			}
		}
		if n.waitErr != nil {
			log, _ := os.ReadFile(n.logPath)
			n.t.Errorf("节点 %d 退出失败：%v\n%s", n.config.ID, n.waitErr, log)
		} else if n.t.Failed() {
			log, _ := os.ReadFile(n.logPath)
			n.t.Logf("节点 %d 日志：\n%s", n.config.ID, log)
		}
	})
}

func (n *clusterProcess) clusterConfig() *types.Config {
	n.t.Helper()
	resp := n.call(clusterProcessRequest{Op: "config"})
	require.NotNil(n.t, resp.Config)
	return resp.Config
}

// 在测试主协程轮询，并共用截止时间，避免 Eventually 后台协程触发 FailNow。
func (n *clusterProcess) waitConfig(condition func(*types.Config) bool) {
	n.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp := n.callUntil(clusterProcessRequest{Op: "config"}, deadline)
		require.NotNil(n.t, resp.Config)
		if condition(resp.Config) {
			return
		}
		time.Sleep(min(50*time.Millisecond, time.Until(deadline)))
	}
	n.t.Fatal("集群配置未在截止时间内达到预期状态")
}

// 故障转移测试等待配置选举收敛后再发消息，不把检测到离线等同于恢复完成。
func (n *clusterProcess) waitFailover(nodeID uint64) {
	n.t.Helper()
	n.waitConfig(func(cfg *types.Config) bool {
		offline := false
		for _, node := range cfg.Nodes {
			if node.Id == nodeID {
				offline = !node.Online
			}
		}
		if !offline {
			return false
		}
		for _, slot := range cfg.Slots {
			if slot.Leader == 0 || slot.Leader == nodeID || slot.Status != types.SlotStatus_SlotStatusNormal {
				return false
			}
		}
		return true
	})
	n.call(clusterProcessRequest{Op: "ready"})
}

func (n *clusterProcess) connect(uid string) *client.Client {
	n.t.Helper()
	cli := client.New(n.config.TCPAddr, client.WithUID(uid))
	n.t.Cleanup(func() { cli.Close() })
	require.NoError(n.t, cli.Connect())
	return cli
}

func processLeader(t *testing.T, nodes []*clusterProcess) *clusterProcess {
	t.Helper()
	leader := nodes[0].call(clusterProcessRequest{Op: "config"}).NodeID
	for _, n := range nodes {
		if n.config.ID == leader {
			return n
		}
	}
	t.Fatalf("集群领导者 %d 不在测试节点中", leader)
	return nil
}

// 先注册接收回调再发送；有界等待避免丢失唤醒后整包一直卡住。
func receiveProcessMessage(t *testing.T, cli *client.Client) <-chan string {
	t.Helper()
	result := make(chan string, 4)
	cli.SetOnRecv(func(packet *wkproto.RecvPacket) error {
		select {
		case result <- string(packet.Payload):
		default:
		}
		return nil
	})
	return result
}

func requireProcessMessage(t *testing.T, messages <-chan string, want string) {
	t.Helper()
	select {
	case got := <-messages:
		require.Equal(t, want, got)
	case <-time.After(10 * time.Second):
		t.Fatalf("未收到跨节点消息 %q", want)
	}
}
