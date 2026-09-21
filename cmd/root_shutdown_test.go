package cmd

import (
	"errors"
	"testing"
)

func TestStopServerPreservesShutdownError(t *testing.T) {
	want := errors.New("notification archive sync failed")
	calls := 0
	err := stopServer(func() error { calls++; return want })
	if !errors.Is(err, want) || calls != 1 {
		t.Fatalf("停止失败应只执行一次并传给退出入口：calls=%d error=%v", calls, err)
	}
}

func TestStopServerSuccess(t *testing.T) {
	calls := 0
	if err := stopServer(func() error { calls++; return nil }); err != nil || calls != 1 {
		t.Fatalf("正常停止应返回成功：calls=%d error=%v", calls, err)
	}
}
