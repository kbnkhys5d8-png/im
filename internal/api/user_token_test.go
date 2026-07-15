package api

import (
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
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
