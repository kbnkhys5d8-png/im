package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSearchSourceBootstrapStartupTimeoutCoversRollingRebalance(t *testing.T) {
	if searchSourceBootstrapStartupTimeout < 20*time.Minute {
		t.Fatalf("startup timeout = %v, want at least twenty minutes for formal inventory convergence", searchSourceBootstrapStartupTimeout)
	}
}

func TestInitializeSearchSourceBeforeTrafficTimesOutAndReturnsControl(t *testing.T) {
	var recorded error
	started := time.Now()
	err := initializeSearchSourceBeforeTraffic(
		context.Background(),
		20*time.Millisecond,
		func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		func(err error) { recorded = err },
	)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(recorded, context.DeadlineExceeded) {
		t.Fatalf("errors = returned:%v recorded:%v, want deadline exceeded", err, recorded)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded startup bootstrap took %v", elapsed)
	}
	// Reaching here models the subsequent engine/API/plugin startup: bootstrap
	// failure is recorded for search readiness but is not returned by Start.
}

func TestInitializeSearchSourceCompletesBeforeTrafficContinuation(t *testing.T) {
	events := make([]string, 0, 3)
	err := initializeSearchSourceBeforeTraffic(
		context.Background(), time.Second,
		func(context.Context) error {
			events = append(events, "bootstrap")
			return nil
		},
		func(error) { events = append(events, "readiness") },
	)
	if err != nil {
		t.Fatal(err)
	}
	events = append(events, "traffic")
	want := []string{"bootstrap", "readiness", "traffic"}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("startup order = %v, want %v", events, want)
		}
	}
}
