package wklog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"go.uber.org/zap"
)

func TestLoggerConcurrentColdStart(t *testing.T) {
	if os.Getenv("WKLOG_TEST_COLD_START") != "1" {
		// 使用独立进程保留真实的未初始化状态，不让其他测试提前配置而掩盖竞争。
		cmd := exec.Command(os.Args[0], "-test.run=^TestLoggerConcurrentColdStart$", "-test.timeout=15s")
		cmd.Dir = t.TempDir()
		cmd.Env = append(os.Environ(), "WKLOG_TEST_COLD_START=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("concurrent cold start failed: %v\n%s", err, output)
		}
		return
	}
	start := make(chan struct{})
	var workers sync.WaitGroup
	operations := loggerOperations()
	for i := 0; i < 32; i++ {
		operation := operations[i%len(operations)]
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			operation()
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		Configure(&Options{Level: zap.WarnLevel, LogDir: "explicit", NoStdout: true, TraceOn: true})
	}()
	close(start)
	workers.Wait()
	if Level() != zap.WarnLevel {
		t.Fatal("default initialization overwrote explicit configuration")
	}
	NewWKLog("cold").Trace("explicit marker", "cold-start")
	entries := readLoggerEntries(t, filepath.Join("explicit", "trace.log"))
	if len(entries) != 0 {
		t.Fatal("trace output ignored the explicitly configured warning level")
	}
	Warn("explicit marker")
	entries = readLoggerEntries(t, filepath.Join("explicit", "warn.log"))
	if !hasLoggerMessage(entries, "explicit marker") {
		t.Fatal("explicit log directory was overwritten")
	}
}

func TestLoggerConcurrentConfigureAndRead(t *testing.T) {
	dir := t.TempDir()
	Configure(&Options{LogDir: dir, NoStdout: true})
	start := make(chan struct{})
	var workers sync.WaitGroup
	for _, operation := range loggerOperations() {
		workers.Add(1)
		go func(operation func()) {
			defer workers.Done()
			<-start
			for i := 0; i < 20; i++ {
				operation()
			}
		}(operation)
	}
	for i := 0; i < 2; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for j := 0; j < 20; j++ {
				Configure(&Options{LogDir: dir, NoStdout: true, TraceOn: j%2 == 0})
			}
		}()
	}
	close(start)
	workers.Wait()
	finalDir := t.TempDir()
	Configure(&Options{Level: zap.WarnLevel, LogDir: finalDir, NoStdout: true})
	if Level() != zap.WarnLevel {
		t.Fatal("final explicit configuration was not published")
	}
	Info("filtered final info")
	Warn("final warning")
	if !hasLoggerMessage(readLoggerEntries(t, filepath.Join(finalDir, "warn.log")), "final warning") {
		t.Fatal("final configured writer was not used")
	}
	if len(readLoggerEntries(t, filepath.Join(finalDir, "info.log"))) != 0 {
		t.Fatal("final log level was not respected")
	}
}

func TestLoggerConfigurationCompatibility(t *testing.T) {
	for _, traceOn := range []bool{false, true} {
		t.Run(fmt.Sprintf("trace_%t", traceOn), func(t *testing.T) {
			dir := t.TempDir()
			config := &Options{Level: zap.InfoLevel, LogDir: dir, NoStdout: true, LineNum: true, TraceOn: traceOn}
			Configure(config)
			// 配置发布后由日志组件持有快照，不再借用调用者可变的 Options。
			config.Level = zap.DebugLevel
			config.TraceOn = !traceOn
			log := NewWKLog("compat")
			log.Info("info")
			log.Debug("filtered debug")
			log.Error("error")
			log.Warn("warn")
			log.Foucs("focus")
			Trace("raw trace")
			_, file, line, _ := runtime.Caller(0)
			log.Trace("trace", "test-action", zap.String("extra", "value"))
			traceLocation := fmt.Sprintf("%s:%d", file, line+1)
			_, _, line, _ = runtime.Caller(0)
			log.MessageTrace("message", "message-no", "test-action")
			messageLocation := fmt.Sprintf("%s:%d", file, line+1)
			func() {
				defer func() {
					if got := recover(); got != "【compat】panic" {
						t.Errorf("panic behavior changed: %v", got)
					}
				}()
				log.Panic("panic")
			}()
			if err := Sync(); err != nil {
				t.Fatalf("Sync return contract changed: %v", err)
			}
			for filename, message := range map[string]string{
				"info.log": "【compat】info", "error.log": "【compat】error",
				"warn.log": "【compat】warn", "focus.log": "【compat】focus", "panic.log": "【compat】panic",
			} {
				entries := readLoggerEntries(t, filepath.Join(dir, filename))
				if len(entries) != 1 || entries[0]["msg"] != message {
					t.Errorf("%s routing changed: %+v", filename, entries)
				}
			}
			traces := readLoggerEntries(t, filepath.Join(dir, "trace.log"))
			want := 1
			if traceOn {
				want = 3
			}
			if len(traces) != want || !hasLoggerMessage(traces, "raw trace") {
				t.Fatalf("TraceOn behavior changed: %+v", traces)
			}
			for _, entry := range traces {
				switch entry["msg"] {
				case "【compat】trace":
					if entry["linenum"] != traceLocation || entry["action"] != "test-action" || entry["extra"] != "value" {
						t.Errorf("trace caller or fields changed: %+v", entry)
					}
				case "【compat】message":
					if entry["linenum"] != messageLocation || entry["no"] != "message-no" {
						t.Errorf("message trace caller or fields changed: %+v", entry)
					}
				}
			}
		})
	}
}

func loggerOperations() []func() {
	log := NewWKLog("concurrent")
	return []func(){
		func() { Info("info") }, func() { Debug("debug") }, func() { Trace("trace") },
		func() { Error("error") }, func() { Warn("warn") }, func() { Foucs("focus") },
		func() { _ = Level() }, func() { _ = Sync() },
		func() { log.Trace("trace", "concurrent") },
		func() { log.MessageTrace("message", "no", "concurrent") },
	}
}

func readLoggerEntries(t *testing.T, filename string) []map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return []map[string]interface{}{}
	}
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]map[string]interface{}, 0)
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		entry := make(map[string]interface{})
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return entries
}

func hasLoggerMessage(entries []map[string]interface{}, message string) bool {
	for _, entry := range entries {
		if entry["msg"] == message {
			return true
		}
	}
	return false
}

func TestLogger(t *testing.T) {
	opts := NewOptions()
	opts.Level = zap.DebugLevel
	opts.LineNum = true
	Configure(opts)

	Info("this is info")
	Debug("this is debug")
	Error("this is error", zap.String("key", "value"))
}
