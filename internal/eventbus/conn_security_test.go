package eventbus

import (
	"strings"
	"testing"
)

func TestConnStringDoesNotExposeEncryptionMaterial(t *testing.T) {
	const secret = "sensitive-session-key"
	conn := &Conn{
		Uid:    "user-1",
		AesKey: []byte(secret),
		AesIV:  []byte(secret),
	}
	got := conn.String()
	if strings.Contains(got, secret) || strings.Contains(got, "AesKey") || strings.Contains(got, "AesIV") {
		t.Fatalf("connection string exposes encryption material: %s", got)
	}
}
