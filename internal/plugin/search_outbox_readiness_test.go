package plugin

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestSearchOutboxRuntimeReadinessIsIndependentAndFailsClosed(t *testing.T) {
	injected := errors.New("runtime search outbox is unavailable")
	tests := []struct {
		name           string
		setSourceReady func(*Server)
		setOutboxReady func(*Server)
		wantAvailable  bool
	}{
		{
			name: "missing outbox checker fails closed despite source ready",
			setSourceReady: func(server *Server) {
				server.SetSearchSourceRuntimeReady(func() error { return nil })
			},
		},
		{
			name: "outbox checker error fails closed despite source ready",
			setSourceReady: func(server *Server) {
				server.SetSearchSourceRuntimeReady(func() error { return nil })
			},
			setOutboxReady: func(server *Server) {
				server.SetSearchOutboxRuntimeReady(func() error { return injected })
			},
		},
		{
			name: "outbox ready remains available while source is unready",
			setSourceReady: func(server *Server) {
				server.SetSearchSourceRuntimeReady(func() error { return injected })
			},
			setOutboxReady: func(server *Server) {
				server.SetSearchOutboxRuntimeReady(func() error { return nil })
			},
			wantAvailable: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			server := NewServer(NewOptions(
				WithDir(filepath.Join(root, "plugins")),
				WithSocketPath(filepath.Join(root, "run", "plugin.sock")),
			))
			store := &fakeSearchOutboxStore{}
			server.rpc.searchOutboxStore = store
			server.rpc.searchOutboxNodeID = func() uint64 { return 9 }
			if test.setSourceReady != nil {
				test.setSourceReady(server)
			}
			if test.setOutboxReady != nil {
				test.setOutboxReady(server)
			}

			response, err := server.rpc.searchOutboxPull(searchOutboxPullRequest{
				Version:  searchOutboxProtocolVersion,
				Limit:    1,
				MaxBytes: 1,
			})
			if test.wantAvailable {
				if err != nil {
					t.Fatalf("runtime-ready outbox pull error = %v, want nil", err)
				}
				if response.Version != searchOutboxProtocolVersion ||
					store.pullCalls != 1 {
					t.Fatalf(
						"runtime-ready response = %+v, store calls = %d",
						response,
						store.pullCalls,
					)
				}
				return
			}
			if err == nil {
				t.Fatal("runtime-unready outbox pull was accepted")
			}
			if store.pullCalls != 0 {
				t.Fatalf(
					"runtime-unready outbox pull touched store %d times",
					store.pullCalls,
				)
			}
		})
	}
}
