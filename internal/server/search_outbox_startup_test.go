package server

import (
	"bytes"
	"os"
	"testing"
)

func TestRecoverSearchOutboxRunsBeforeTrafficStartup(t *testing.T) {
	source, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	clusterStart := bytes.Index(source, []byte("err = s.clusterServer.Start()"))
	recovery := bytes.Index(source, []byte("s.clusterServer.RecoverSearchOutbox(s.ctx)"))
	engineStart := bytes.Index(source, []byte("err = s.engine.Start()"))
	if clusterStart < 0 || recovery < 0 || engineStart < 0 {
		t.Fatalf(
			"startup markers = cluster:%d recovery:%d engine:%d",
			clusterStart,
			recovery,
			engineStart,
		)
	}
	if !(clusterStart < recovery && recovery < engineStart) {
		t.Fatalf(
			"startup order = cluster:%d recovery:%d engine:%d",
			clusterStart,
			recovery,
			engineStart,
		)
	}
	if bytes.Contains(source[recovery:engineStart], []byte("return ")) {
		t.Fatal("search outbox recovery failure still blocks ordinary traffic startup")
	}
	for _, obsolete := range [][]byte{
		[]byte("initializeSearchSourceBeforeTraffic"),
		[]byte("ApplySearchSourceOfflineBootstrapMarker"),
	} {
		if bytes.Contains(source, obsolete) {
			t.Fatalf("obsolete startup path remains: %q", obsolete)
		}
	}
}
