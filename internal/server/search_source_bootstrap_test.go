package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/plugin"
)

func TestSearchSourceBootstrapStartupHasNoFixedDeadline(t *testing.T) {
	if searchSourceBootstrapStartupTimeout != 0 {
		t.Fatalf("startup timeout = %v, want no fixed deadline", searchSourceBootstrapStartupTimeout)
	}
}

func TestInitializeSearchSourceBeforeTrafficTimesOutAndReturnsControl(t *testing.T) {
	var recorded error
	started := time.Now()
	err := initializeSearchSourceBeforeTraffic(
		context.Background(),
		20*time.Millisecond,
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		func(err error) { recorded = err },
	)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(recorded, context.DeadlineExceeded) {
		t.Fatalf("errors = returned:%v recorded:%v, want deadline exceeded", err, recorded)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded startup bootstrap took %v", elapsed)
	}
	// Reaching here models the subsequent engine/API/plugin startup: bootstrap
	// failure is recorded for search readiness but is not returned by Start.
}

func TestInitializeSearchSourceWithoutDeadlineWaitsForCompletion(t *testing.T) {
	started := make(chan struct{})
	completed := make(chan struct{})
	returned := make(chan error, 1)
	go func() {
		returned <- initializeSearchSourceBeforeTraffic(
			context.Background(),
			0,
			func(ctx context.Context) error {
				if _, ok := ctx.Deadline(); ok {
					t.Error("unbounded bootstrap context has a deadline")
				}
				close(started)
				<-completed
				return nil
			},
			func(error) {},
		)
	}()

	<-started
	select {
	case err := <-returned:
		t.Fatalf("bootstrap returned before apply completed: %v", err)
	default:
	}
	close(completed)
	if err := <-returned; err != nil {
		t.Fatalf("bootstrap returned %v, want nil", err)
	}
}

func TestInitializeSearchSourceWithoutDeadlineStopsOnParentCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	var recorded []error
	err := initializeSearchSourceBeforeTraffic(
		parent,
		0,
		func(ctx context.Context) error {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		},
		func(err error) { recorded = append(recorded, err) },
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if len(recorded) != 1 || recorded[0] != err {
		t.Fatalf("recorded = %v, want exactly the returned cancellation", recorded)
	}
}

func TestSearchSourceAuthorizedApplyingFailureStopsBeforeTrafficStartup(t *testing.T) {
	s := NewTestServer(t)
	s.opts.Mode = options.TestMode
	markerPath := filepath.Join(s.opts.DataDir, plugin.SearchSourceOfflineBootstrapMarkerName)
	if err := os.MkdirAll(filepath.Dir(markerPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(markerPath+".applying", []byte(`{"version":1,"node_id":1001}`+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.cancel()
	t.Cleanup(s.StopNoErr)

	err := s.Start()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Start error = %v, want context canceled", err)
	}
	assertTrafficIsStopped(t, s)
}

func TestSearchSourceBootstrapRequiredStillAllowsOrdinaryTrafficStartup(t *testing.T) {
	for _, test := range []struct {
		name    string
		prepare func(t *testing.T, markerPath string)
	}{
		{
			name:    "no marker",
			prepare: func(*testing.T, string) {},
		},
		{
			name: "window closed without recovery",
			prepare: func(t *testing.T, markerPath string) {
				if err := os.MkdirAll(filepath.Dir(markerPath), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(markerPath+".window-closed", []byte(`{"version":1,"node_id":1001}`+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := NewTestServer(t)
			s.opts.Mode = options.TestMode
			markerPath := filepath.Join(s.opts.DataDir, plugin.SearchSourceOfflineBootstrapMarkerName)
			test.prepare(t, markerPath)
			if err := s.Start(); err != nil {
				t.Fatalf("ordinary startup returned %v", err)
			}
			t.Cleanup(s.StopNoErr)
			assertTrafficIsRunning(t, s)
			if _, err := os.Stat(markerPath + ".window-closed"); err != nil {
				t.Fatalf("required bootstrap state was not retained: %v", err)
			}
		})
	}
}

func assertTrafficIsStopped(t *testing.T, s *Server) {
	t.Helper()
	if conn, err := net.DialTimeout("tcp", s.opts.External.TCPAddr, 100*time.Millisecond); err == nil {
		conn.Close()
		t.Fatal("engine accepted traffic after bootstrap failure")
	}
	client := http.Client{Timeout: 100 * time.Millisecond}
	if resp, err := client.Get("http://" + s.opts.HTTPAddr + "/health"); err == nil {
		resp.Body.Close()
		t.Fatal("API accepted traffic after bootstrap failure")
	}
	if _, err := os.Stat(s.opts.Plugin.SocketPath); !os.IsNotExist(err) {
		t.Fatalf("plugin socket state = %v, want not started", err)
	}
}

func assertTrafficIsRunning(t *testing.T, s *Server) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", s.opts.External.TCPAddr, time.Second)
	if err != nil {
		t.Fatalf("ordinary IM listener did not start: %v", err)
	}
	conn.Close()
}
