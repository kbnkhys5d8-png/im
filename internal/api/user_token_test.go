package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkhttp"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestDeviceTokenUpdateRequired(t *testing.T) {
	tests := []struct {
		name   string
		device wkdb.Device
		req    UpdateTokenReq
		want   bool
	}{
		{
			name: "missing device",
			req: UpdateTokenReq{
				UID:         "user-1",
				Token:       "token-1",
				DeviceLevel: wkproto.DeviceLevelMaster,
			},
			want: true,
		},
		{
			name: "same token and level",
			device: wkdb.Device{
				Uid:         "user-1",
				Token:       "token-1",
				DeviceLevel: uint8(wkproto.DeviceLevelMaster),
			},
			req: UpdateTokenReq{
				UID:         "user-1",
				Token:       "token-1",
				DeviceLevel: wkproto.DeviceLevelMaster,
			},
			want: false,
		},
		{
			name: "changed token",
			device: wkdb.Device{
				Uid:         "user-1",
				Token:       "token-1",
				DeviceLevel: uint8(wkproto.DeviceLevelMaster),
			},
			req: UpdateTokenReq{
				UID:         "user-1",
				Token:       "token-2",
				DeviceLevel: wkproto.DeviceLevelMaster,
			},
			want: true,
		},
		{
			name: "changed device level",
			device: wkdb.Device{
				Uid:         "user-1",
				Token:       "token-1",
				DeviceLevel: uint8(wkproto.DeviceLevelSlave),
			},
			req: UpdateTokenReq{
				UID:         "user-1",
				Token:       "token-1",
				DeviceLevel: wkproto.DeviceLevelMaster,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := deviceTokenUpdateRequired(tt.device, tt.req); got != tt.want {
				t.Fatalf("deviceTokenUpdateRequired() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUpdateTokenLogFieldsDoNotContainToken(t *testing.T) {
	const secret = "do-not-log-this-token"
	fields := updateTokenLogFields(UpdateTokenReq{
		UID:         "user-1",
		Token:       secret,
		DeviceFlag:  wkproto.APP,
		DeviceLevel: wkproto.DeviceLevelMaster,
	})
	for _, field := range fields {
		if strings.Contains(strings.ToLower(field.Key), "token") {
			t.Fatalf("sensitive log field key %q", field.Key)
		}
		if field.String == secret {
			t.Fatal("token value must not be included in log fields")
		}
	}
}

func TestTokenUpdateLockKeyIncludesDeviceFlag(t *testing.T) {
	appKey := tokenUpdateLockKey("user-1", wkproto.APP)
	webKey := tokenUpdateLockKey("user-1", wkproto.WEB)

	if appKey == webKey {
		t.Fatalf("different device flags must not share the same token update key: %q", appKey)
	}
	if appKey != "user-1:0" {
		t.Fatalf("unexpected app token update key: %q", appKey)
	}
}

func TestTokenVersionMatchesRejectsLateRevocation(t *testing.T) {
	device := wkdb.Device{Uid: "u", Token: "current", DeviceLevel: 1, TokenVersion: 42}
	for _, tc := range []struct {
		version uint64
		token   string
		want    bool
	}{
		{41, "revoke", false}, {0, "legacy", false}, {42, "revoke", false},
		{42, "current", true}, {43, "new", true},
	} {
		req := UpdateTokenReq{UID: "u", Token: tc.token, DeviceLevel: 1, TokenVersion: tc.version}
		if got := tokenVersionMatches(device, req); got != tc.want {
			t.Fatalf("version=%d: got %v, want %v", tc.version, got, tc.want)
		}
	}
	if !tokenVersionMatches(wkdb.Device{Uid: "u", Token: "old"}, UpdateTokenReq{UID: "u", Token: "legacy"}) {
		t.Fatal("尚未使用版本协议的旧设备应保留兼容")
	}
	if !deviceTokenUpdateRequired(device, UpdateTokenReq{UID: "u", Token: "current", DeviceLevel: 1, TokenVersion: 43}) {
		t.Fatal("复用相同 Token 时也必须保存更高版本")
	}
}

func TestTokenVersionResponseSurvivesForwarding(t *testing.T) {
	backend := wkhttp.New()
	backend.POST("/user/token/fenced", func(c *wkhttp.Context) {
		respondTokenUpdated(c, UpdateTokenReq{TokenVersion: 42})
	})
	server := httptest.NewServer(backend.GetGinRoute())
	defer server.Close()
	proxy := wkhttp.New()
	proxy.POST("/user/token/fenced", func(c *wkhttp.Context) {
		c.ForwardWithBody(server.URL+"/user/token/fenced", []byte(`{}`))
	})
	w := httptest.NewRecorder()
	proxy.GetGinRoute().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/user/token/fenced", strings.NewReader(`{}`)))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", w.Code, w.Body.String())
	}
	var body struct {
		Version uint64 `json:"token_version"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Version != 42 {
		t.Fatalf("转发丢失版本确认: %s, err=%v", w.Body.String(), err)
	}
}

func TestTokenVersionRouteRejectsMissingVersion(t *testing.T) {
	router := wkhttp.New()
	newUser(nil).route(router)
	w := httptest.NewRecorder()
	router.GetGinRoute().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/user/token/fenced", strings.NewReader(`{"uid":"u","token":"t"}`)))
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "token_version") {
		t.Fatalf("缺少版本必须在接触存储前失败: status=%d body=%s", w.Code, w.Body.String())
	}
}
