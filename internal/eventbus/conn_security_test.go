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

func TestConnEncodeDecodePreservesInternalMarker(t *testing.T) {
	conn := &Conn{
		Uid:      "system-user",
		DeviceId: "____device",
		Internal: true,
	}

	data, err := conn.Encode()
	if err != nil {
		t.Fatalf("encode connection: %v", err)
	}

	var decoded Conn
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("decode connection: %v", err)
	}
	if !decoded.Internal {
		t.Fatal("server-internal marker was lost during connection encoding")
	}
}

func TestConnDecodeAcceptsLegacyEncodingWithoutInternalMarker(t *testing.T) {
	conn := &Conn{
		Uid:      "legacy-user",
		DeviceId: "legacy-device",
	}
	data, err := conn.Encode()
	if err != nil {
		t.Fatalf("encode connection: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("encoded connection is empty")
	}

	var decoded Conn
	if err := decoded.Decode(data[:len(data)-1]); err != nil {
		t.Fatalf("decode legacy connection: %v", err)
	}
	if decoded.Internal {
		t.Fatal("legacy connection must not become server-internal")
	}
}
