package handler

import (
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestTokenVerifyFailureLogFieldsDoNotContainTokens(t *testing.T) {
	const secret = "do-not-log-this-token"
	fields := tokenVerifyFailureLogFields("user-1", 1002, wkproto.APP)
	for _, field := range fields {
		if strings.Contains(strings.ToLower(field.Key), "token") {
			t.Fatalf("sensitive log field key %q", field.Key)
		}
		if field.String == secret {
			t.Fatal("token value must not be included in log fields")
		}
	}
}

func TestSafeConnectionLogFieldsExcludeEncryptionMaterial(t *testing.T) {
	const secret = "sensitive-session-key"
	fields := safeConnectionLogFields(&eventbus.Conn{
		Uid:        "user-1",
		NodeId:     1002,
		ConnId:     99,
		DeviceId:   "private-device-id",
		DeviceFlag: wkproto.APP,
		AesKey:     []byte(secret),
		AesIV:      []byte(secret),
	})
	for _, field := range fields {
		key := strings.ToLower(field.Key)
		if strings.Contains(key, "aes") || strings.Contains(key, "token") || strings.Contains(key, "deviceid") {
			t.Fatalf("sensitive log field key %q", field.Key)
		}
		if field.String == secret || field.String == "private-device-id" {
			t.Fatalf("sensitive log field value exposed by %q", field.Key)
		}
	}
}
