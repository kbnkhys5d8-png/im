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

func TestSearchReadinessWiringUsesSeparateCallbacks(t *testing.T) {
	source, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	sourceWiring := []byte(
		"s.pluginServer.SetSearchSourceRuntimeReady(s.clusterServer.SearchSourceReady)",
	)
	outboxWiring := []byte(
		"s.pluginServer.SetSearchOutboxRuntimeReady(s.clusterServer.SearchOutboxReady)",
	)
	if count := bytes.Count(source, sourceWiring); count != 1 {
		t.Fatalf("search source readiness wiring count = %d, want 1", count)
	}
	if count := bytes.Count(source, outboxWiring); count != 1 {
		t.Fatalf("search outbox readiness wiring count = %d, want 1", count)
	}
	if bytes.Contains(
		source,
		[]byte(
			"s.pluginServer.SetSearchSourceRuntimeReady(s.clusterServer.SearchOutboxReady)",
		),
	) {
		t.Fatal("search source readiness is cross-wired to search outbox recovery")
	}
}
