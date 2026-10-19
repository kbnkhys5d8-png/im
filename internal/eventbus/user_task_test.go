package eventbus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
)

type taskTestQueue struct {
	IUser
	events  chan *Event
	stopped chan struct{}
}

func (q *taskTestQueue) AddEvent(_ string, event *Event)  { q.events <- event }
func (*taskTestQueue) Advance(string)                     {}
func (q *taskTestQueue) UserTaskStopped() <-chan struct{} { return q.stopped }

func TestUserTaskCancellationStopAndRejectedQueue(t *testing.T) {
	old := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = old })
	for _, mode := range []string{"cancel-queued", "stop-queued", "already-stopped", "rejected-or-saturated"} {
		t.Run(mode, func(t *testing.T) {
			q := &taskTestQueue{events: make(chan *Event, 1), stopped: make(chan struct{})}
			user := newUserPlus(q)
			var calls atomic.Int32
			ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer cancel()
			if mode == "already-stopped" {
				close(q.stopped)
			}
			done := make(chan error, 1)
			go func() { done <- user.RunOnUserLeader(ctx, "task-user", func() error { calls.Add(1); return nil }) }()
			var event *Event
			if mode != "already-stopped" {
				select {
				case event = <-q.events:
				case <-time.After(time.Second):
					t.Fatal("任务未入队")
				}
			}
			switch mode {
			case "cancel-queued":
				cancel()
			case "stop-queued":
				close(q.stopped)
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("丢弃、取消或停机任务未失败")
				}
			case <-time.After(time.Second):
				t.Fatal("队列未执行任务时调用方永久等待")
			}
			if event != nil {
				event.UserLeaderTask.Execute(true)
			}
			if calls.Load() != 0 {
				t.Fatal("取消后任务晚到仍执行写入")
			}
		})
	}
}

func TestUserTaskRunningCancellationDoesNotBlockCompletion(t *testing.T) {
	old := options.G
	options.G = options.New()
	t.Cleanup(func() { options.G = old })
	q := &taskTestQueue{events: make(chan *Event, 1), stopped: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- newUserPlus(q).RunOnUserLeader(ctx, "task-user", func() error { close(started); <-release; return nil })
	}()
	event := <-q.events
	finished := make(chan struct{})
	go func() { event.UserLeaderTask.Execute(true); close(finished) }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("取消未返回: %v", err)
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("调用方返回后完成通知阻塞了 UID 队列")
	}
}

func TestLocalUserTaskCannotBeEncoded(t *testing.T) {
	if _, err := (EventBatch{&Event{Type: EventUserLeaderTask}}).Encode(); err == nil {
		t.Fatal("本地闭包任务被编码转发")
	}
}
