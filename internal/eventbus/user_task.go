package eventbus

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"go.uber.org/zap"
)

// UserTask 的完成通知独立于请求生命周期；取消后尚未开始的任务不得晚到写入。
type UserTask struct {
	ctx        context.Context
	fn         func() error
	leaderOnly bool
	state      atomic.Uint32
	once       sync.Once
	done       chan error
}

func (t *UserTask) finish(err error) { t.once.Do(func() { t.done <- err }) }

func (t *UserTask) Execute(isLeader bool) {
	if !t.state.CompareAndSwap(0, 1) {
		return
	}
	defer func() {
		if value := recover(); value != nil {
			wklog.Error("认证队列任务异常", zap.Any("panic", value), zap.Stack("stack"))
			t.finish(errors.New("认证队列任务执行失败"))
		}
	}()
	if err := t.ctx.Err(); err != nil {
		t.finish(err)
		return
	}
	if t.leaderOnly && !isLeader {
		t.finish(errors.New("认证节点已变化，请重试"))
		return
	}
	t.finish(t.fn())
}

// RunOnUserLeader 必须从队列外调用；仅在当前认证 leader 的 UID 队列执行。
func (u *userPlus) RunOnUserLeader(ctx context.Context, uid string, fn func() error) error {
	return u.runTask(ctx, uid, true, fn)
}

// RunOnUserLocal 用于源节点精确关闭连接，不要求源节点同时是认证 leader。
func (u *userPlus) RunOnUserLocal(ctx context.Context, uid string, fn func() error) error {
	return u.runTask(ctx, uid, false, fn)
}

func (u *userPlus) runTask(ctx context.Context, uid string, leaderOnly bool, fn func() error) error {
	timeout := options.G.Cluster.ReqTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	task := &UserTask{ctx: ctx, fn: fn, leaderOnly: leaderOnly, done: make(chan error, 1)}
	stopped := u.user.UserTaskStopped()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stopped:
		return errors.New("用户事件队列已停止")
	default:
	}
	u.AddEvent(uid, &Event{Type: EventUserLeaderTask, UserLeaderTask: task})
	u.Advance(uid)
	select {
	case err := <-task.done:
		return err
	case <-ctx.Done():
		task.state.CompareAndSwap(0, 2)
		return ctx.Err()
	case <-stopped:
		task.state.CompareAndSwap(0, 2)
		return errors.New("用户事件队列已停止")
	}
}
