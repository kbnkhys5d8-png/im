package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

// 两个独立进程隔离全局服务；仅把集群传输和持久化替换为 mock。
func TestTokenUpdateAndConnectMeetOnRemoteLeader(t *testing.T) {
	for _, mode := range []string{"inflight", "takeover-empty-index"} {
		t.Run(mode, func(t *testing.T) { runRemoteLeaderSessionRace(t, mode) })
	}
}

func runRemoteLeaderSessionRace(t *testing.T, mode string) {
	if os.Getenv("WK_SESSION_RACE_LEADER") == "1" {
		t.Skip("子进程")
	}
	fixture := newSessionRaceFixture(t)
	sourceMux := http.NewServeMux()
	sourceMux.HandleFunc("/test/events", func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fixture.handler.OnMessage(&proto.Message{MsgType: 2001, Content: data})
	})
	sourceMux.HandleFunc("/test/revoke", func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		conn := &eventbus.Conn{}
		if err := conn.Decode(data); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if err := ingress.New().RevokeConn(conn); err != nil {
			http.Error(w, err.Error(), 500)
		}
	})
	sourceMux.HandleFunc("/test/conns", func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		body, err := ingress.New().UserConns(string(data))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write(body)
	})
	source := httptest.NewServer(sourceMux)
	defer source.Close()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSessionRaceLeaderProcess$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "WK_SESSION_RACE_LEADER=1", "WK_SESSION_RACE_SOURCE="+source.URL, "WK_SESSION_RACE_MODE="+mode)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	address := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if value, ok := strings.CutPrefix(scanner.Text(), "SESSION_RACE_LEADER="); ok {
				address <- value
			}
		}
	}()
	var url string
	select {
	case url = <-address:
	case <-time.After(10 * time.Second):
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatalf("认证 leader 启动失败: %s", stderr.String())
	}
	t.Cleanup(func() {
		sessionRaceControl(t, url+"/test/resume")
		sessionRaceControl(t, url+"/test/stop")
		if err := cmd.Wait(); err != nil {
			t.Errorf("认证 leader 进程失败: %v %s", err, stderr.String())
		}
	})
	fixture.cluster.leader.Store(2)
	fixture.cluster.forwardURL = url
	conn := sessionRaceConn(100)
	conn.Auth = mode == "takeover-empty-index"
	fixture.real.SetContext(conn)
	if mode == "inflight" {
		fixture.handler.OnEvent(&eventbus.UserContext{Uid: conn.Uid, EventType: eventbus.EventConnect,
			Events: []*eventbus.Event{{Type: eventbus.EventConnect, Conn: conn, SourceNodeId: 1,
				Frame: &wkproto.ConnectPacket{UID: conn.Uid, Token: "old-token", DeviceFlag: wkproto.APP, DeviceID: conn.DeviceId, Version: 3}}},
		})
		sessionRaceControl(t, url+"/test/paused")
	} else {
		// 源会话已被旧 leader 认证；新 leader 从未处理过它的 CONNECT，索引为空。
		fixture.users.UpdateConn(conn)
	}
	updated := make(chan *httptest.ResponseRecorder, 1)
	go func() { updated <- fixture.update("revoke-token", 42) }()
	sessionRaceControl(t, url+"/test/queued")
	if mode == "inflight" {
		select {
		case response := <-updated:
			t.Fatalf("远端登记未结束，Token 更新却已返回: %d", response.Code)
		default:
		}
	}
	sessionRaceControl(t, url+"/test/resume")
	select {
	case response := <-updated:
		if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"token_version":42`) {
			t.Fatalf("转发更新失败: %d %s", response.Code, response.Body.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("远端 Token 更新未返回")
	}
	resp, err := http.Get(url + "/test/state")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var state struct {
		Version uint64
		Closed  bool
	}
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		t.Fatal(err)
	}
	if state.Version != 42 || !state.Closed || !fixture.real.closed.Load() {
		t.Fatalf("远端在途连接逃过撤销: %+v", state)
	}
	device, _ := fixture.db.GetDevice(conn.Uid, 0)
	if device.TokenVersion != 41 {
		t.Fatal("入口节点写入了本地 Token，而没有交给认证 leader")
	}
}

func sessionRaceControl(t *testing.T, url string) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("测试控制失败: %s: %d", url, resp.StatusCode)
	}
}

func TestSessionRaceLeaderProcess(t *testing.T) {
	if os.Getenv("WK_SESSION_RACE_LEADER") != "1" {
		t.Skip("仅供双进程测试启动")
	}
	fixture := newSessionRaceFixtureNode(t, 2)
	fixture.cluster.revokeURL = os.Getenv("WK_SESSION_RACE_SOURCE")
	fixture.cluster.forwardURL = os.Getenv("WK_SESSION_RACE_SOURCE")
	paused, resume, stop := make(chan struct{}), make(chan struct{}), make(chan struct{})
	if os.Getenv("WK_SESSION_RACE_MODE") == "inflight" {
		fixture.db.channelGate = func() { close(paused); <-resume }
	}
	fixture.users.taskQueued = make(chan struct{})
	var resumeOnce sync.Once
	mux := http.NewServeMux()
	mux.Handle("/user/", fixture.router.GetGinRoute())
	mux.HandleFunc("/test/events", func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		fixture.handler.OnMessage(&proto.Message{MsgType: 2001, Content: data})
	})
	mux.HandleFunc("/test/paused", func(w http.ResponseWriter, r *http.Request) { <-paused })
	mux.HandleFunc("/test/queued", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-fixture.users.taskQueued:
		case <-time.After(3 * time.Second):
			w.WriteHeader(http.StatusRequestTimeout)
		}
	})
	mux.HandleFunc("/test/resume", func(w http.ResponseWriter, r *http.Request) { resumeOnce.Do(func() { close(resume) }) })
	mux.HandleFunc("/test/stop", func(w http.ResponseWriter, r *http.Request) { close(stop) })
	mux.HandleFunc("/test/state", func(w http.ResponseWriter, r *http.Request) {
		device, _ := fixture.db.GetDevice("session-race-user", 0)
		json.NewEncoder(w).Encode(struct {
			Version uint64
			Closed  bool
		}{device.TokenVersion, fixture.cluster.remoteClosed.Load() && fixture.users.ConnById("session-race-user", 1, 99) == nil})
	})
	server := httptest.NewServer(mux)
	defer server.Close()
	fmt.Println("SESSION_RACE_LEADER=" + server.URL)
	<-stop
}

func TestQueuedTokenUpdateRejectsChangedLeader(t *testing.T) {
	for _, leader := range []uint64{0, 2} {
		t.Run(fmt.Sprint(leader), func(t *testing.T) {
			fixture := newSessionRaceFixture(t)
			fixture.cluster.leader.Store(leader)
			var ran bool
			done := make(chan error, 1)
			go func() {
				done <- eventbus.User.RunOnUserLeader(context.Background(), "session-race-user", func() error { ran = true; return nil })
			}()
			select {
			case err := <-done:
				if err == nil || ran {
					t.Fatal("失去 leader 后仍然执行了 Token 写任务")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("leader 变化导致排队任务丢失或错误转发")
			}
		})
	}
}
