package plugin

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
)

func TestSearchQueryGateFailsClosedExceptSafeMissingMarker(t *testing.T) {
	safeDir := gateFileInfo{name: "tsdd-search-control", mode: os.ModeDir | 0o700, uid: 0}
	safeMarker := gateFileInfo{name: "usersearch-disabled-v1", mode: 0o644, uid: 0}
	for _, test := range []struct {
		name       string
		configured bool
		value      string
		parent     os.FileInfo
		marker     os.FileInfo
		markerErr  error
		wantOpen   bool
	}{
		{name: "safe missing marker", configured: true, value: searchQueryGatePath, parent: safeDir, markerErr: os.ErrNotExist, wantOpen: true},
		{name: "missing env"},
		{name: "wrong path", configured: true, value: "/tmp/unsafe", parent: safeDir, markerErr: os.ErrNotExist},
		{name: "missing parent", configured: true, value: searchQueryGatePath, markerErr: os.ErrNotExist},
		{name: "writable parent", configured: true, value: searchQueryGatePath, parent: gateFileInfo{name: "parent", mode: os.ModeDir | 0o777, uid: 0}, markerErr: os.ErrNotExist},
		{name: "world-readable parent", configured: true, value: searchQueryGatePath, parent: gateFileInfo{name: "parent", mode: os.ModeDir | 0o755, uid: 0}, markerErr: os.ErrNotExist},
		{name: "safe marker disables", configured: true, value: searchQueryGatePath, parent: safeDir, marker: safeMarker},
		{name: "symlink marker disables", configured: true, value: searchQueryGatePath, parent: safeDir, marker: gateFileInfo{name: "marker", mode: os.ModeSymlink | 0o777, uid: 0}},
		{name: "fifo marker disables", configured: true, value: searchQueryGatePath, parent: safeDir, marker: gateFileInfo{name: "marker", mode: os.ModeNamedPipe | 0o600, uid: 0}},
		{name: "writable marker disables", configured: true, value: searchQueryGatePath, parent: safeDir, marker: gateFileInfo{name: "marker", mode: 0o666, uid: 0}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := inspectSearchQueryGate(test.value, test.configured, func(path string) (os.FileInfo, error) {
				if path == filepath.Dir(searchQueryGatePath) {
					if test.parent == nil {
						return nil, os.ErrNotExist
					}
					return test.parent, nil
				}
				return test.marker, test.markerErr
			})
			if (err == nil) != test.wantOpen {
				t.Fatalf("gate error = %v, wantOpen=%v", err, test.wantOpen)
			}
		})
	}
}

func TestSearchQueryGateReturnsActual503BeforeLegacyPlugin(t *testing.T) {
	server := newSearchQueryHTTPTestServer(t)
	server.searchQueryGate = func(string, string) error { return errSearchQueryUnavailable }
	server.pluginManager.add(newPlugin(server, nil, &pluginproto.PluginInfo{No: searchSourcePluginNo, Name: searchSourcePluginName}))
	response := servePluginRequest(t, server)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("HTTP status = %d, want 503", response.Code)
	}
	if got, want := response.Body.String(), `{"msg":"search is locally disabled","status":503}`; got != want {
		t.Fatalf("body = %q, want %q", got, want)
	}
}

func TestSearchQueryMissingAndOfflinePluginUseActualHTTPStatus(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		server := newSearchQueryHTTPTestServer(t)
		server.searchQueryGate = func(string, string) error { return nil }
		if got := servePluginRequest(t, server).Code; got != http.StatusNotFound {
			t.Fatalf("HTTP status = %d, want 404", got)
		}
	})
	t.Run("offline", func(t *testing.T) {
		server := newSearchQueryHTTPTestServer(t)
		server.searchQueryGate = func(string, string) error { return nil }
		server.pluginManager.add(newPlugin(server, nil, &pluginproto.PluginInfo{No: searchSourcePluginNo, Name: searchSourcePluginName}))
		if got := servePluginRequest(t, server).Code; got != http.StatusServiceUnavailable {
			t.Fatalf("HTTP status = %d, want 503", got)
		}
	})
}

func newSearchQueryHTTPTestServer(t *testing.T) *Server {
	t.Helper()
	root := t.TempDir()
	return NewServer(NewOptions(
		WithDir(filepath.Join(root, "plugins")),
		WithSocketPath(filepath.Join(root, "run", "plugin.sock")),
	))
}

func servePluginRequest(t *testing.T, server *Server) *httptest.ResponseRecorder {
	t.Helper()
	router := wkhttp.New()
	server.SetRoute(router)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/plugins/"+searchSourcePluginNo+searchQueryPluginPath, nil)
	router.GetGinRoute().ServeHTTP(response, request)
	return response
}

type gateFileInfo struct {
	name string
	mode os.FileMode
	uid  uint32
}

func (f gateFileInfo) Name() string       { return f.name }
func (f gateFileInfo) Size() int64        { return 0 }
func (f gateFileInfo) Mode() os.FileMode  { return f.mode }
func (f gateFileInfo) ModTime() time.Time { return time.Time{} }
func (f gateFileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f gateFileInfo) Sys() any           { return &syscall.Stat_t{Uid: f.uid} }
