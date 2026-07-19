package plugin

import (
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wklog"
)

func TestPluginShutdownKeepsRPCAliveThroughStopAndChildExit(t *testing.T) {
	events := make([]string, 0, 5)
	waits := 0
	runPluginShutdown(time.Second, pluginShutdownOps{
		stopPlugins: func() error {
			events = append(events, "plugin-stop-rpc")
			return nil
		},
		waitChildren: func(context.Context) error {
			waits++
			events = append(events, "children-exited")
			return nil
		},
		killChildren: func() { events = append(events, "kill") },
		stopRPC:      func() { events = append(events, "rpc-stop") },
		closeAuth:    func() { events = append(events, "auth-close") },
	})
	want := []string{"plugin-stop-rpc", "children-exited", "rpc-stop", "auth-close"}
	if len(events) != len(want) {
		t.Fatalf("shutdown events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("shutdown events = %v, want %v", events, want)
		}
	}
	if waits != 1 {
		t.Fatalf("wait calls = %d, want 1", waits)
	}
}

func TestPluginShutdownTimeoutKillsOnlyExactRegisteredChildAndWaitsOnce(t *testing.T) {
	managed := exec.Command("sleep", "30")
	if err := managed.Start(); err != nil {
		t.Fatal(err)
	}
	unmanaged := exec.Command("sleep", "30")
	if err := unmanaged.Start(); err != nil {
		_ = managed.Process.Kill()
		_ = managed.Wait()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = unmanaged.Process.Kill()
		_ = unmanaged.Wait()
	})

	server := &Server{children: make(map[*pluginChild]struct{})}
	child := &pluginChild{name: "managed.wkp", cmd: managed, done: make(chan struct{})}
	server.children[child] = struct{}{}
	var waitCalls atomic.Int32
	go func() {
		waitCalls.Add(1)
		_ = managed.Wait()
		server.childMu.Lock()
		delete(server.children, child)
		close(child.done)
		server.childMu.Unlock()
	}()

	waitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	if err := server.waitForPluginChildren(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("initial wait error = %v, want timeout", err)
	}
	cancel()
	server.killPluginChildren()
	waitCtx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.waitForPluginChildren(waitCtx); err != nil {
		t.Fatal(err)
	}
	if waitCalls.Load() != 1 {
		t.Fatalf("cmd.Wait calls = %d, want unique owner", waitCalls.Load())
	}
	if err := unmanaged.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("unregistered child was killed: %v", err)
	}
}

func TestPluginStartRejectedAfterShutdownBoundary(t *testing.T) {
	root := t.TempDir()
	server := &Server{
		opts:       &Options{Dir: root, SocketPath: filepath.Join(root, "plugin.sock")},
		sandboxDir: filepath.Join(root, "sandbox"), stopping: true,
		children: make(map[*pluginChild]struct{}), searchAuth: newLocalPluginAuthorizer(),
		Log: wklog.NewWKLog("plugin.server.test"),
	}
	if err := server.startPluginApp("late.wkp"); err == nil || err.Error() != "plugin server is stopping" {
		t.Fatalf("late start error = %v, want stopping rejection", err)
	}
}
