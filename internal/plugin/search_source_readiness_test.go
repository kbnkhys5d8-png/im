package plugin

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
)

func TestSearchSourceRuntimeReadinessIsTheOnlyRPCGate(t *testing.T) {
	injected := errors.New("runtime search outbox is unavailable")
	tests := []struct {
		name          string
		setRuntime    func(*Server)
		wantAvailable bool
	}{
		{
			name: "missing runtime checker",
		},
		{
			name: "runtime checker returns error",
			setRuntime: func(server *Server) {
				server.SetSearchSourceRuntimeReady(func() error { return injected })
			},
		},
		{
			name: "runtime checker is ready",
			setRuntime: func(server *Server) {
				server.SetSearchSourceRuntimeReady(func() error { return nil })
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
			store := &fakeSearchSourceStore{}
			server.rpc.searchSourceStore = store
			server.rpc.searchSourceNodeID = func() uint64 { return 1 }
			server.rpc.searchSourceRoster = func() ([]uint64, error) {
				return []uint64{1}, nil
			}
			server.rpc.searchSourceAuthority = func(string, uint8) (wkdb.ChannelClusterConfig, error) {
				return validSearchSourceConfig(), nil
			}
			if test.setRuntime != nil {
				test.setRuntime(server)
			}

			response, err := server.rpc.searchSourceChannels(
				searchSourceChannelPageRequest{Version: 5, Limit: 10},
			)
			if test.wantAvailable {
				if err != nil {
					t.Fatalf("runtime-ready RPC error = %v, want nil", err)
				}
				if response.Version != 5 || len(store.calls) == 0 {
					t.Fatalf("runtime-ready RPC response = %+v, store calls = %v", response, store.calls)
				}
				return
			}
			if !errors.Is(err, errSearchSourceUnavailable) {
				t.Fatalf("runtime-unready RPC error = %v, want unavailable", err)
			}
			if len(store.calls) != 0 {
				t.Fatalf("runtime-unready RPC touched store: %v", store.calls)
			}
		})
	}
}
