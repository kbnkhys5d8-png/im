package service

import (
	"context"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
)

// ConnRecovery 在判断离线之前补齐当前用户领导节点的连接目录。
var ConnRecovery IConnRecovery

type IConnRecovery interface {
	Ensure(ctx context.Context, uid string) error
}

// IAuthenticatedConnRecovery 在迟到认证回执成功前确认当前领导已登记真实会话。
type IAuthenticatedConnRecovery interface {
	ConfirmAuthenticated(ctx context.Context, conn *eventbus.Conn, authLeader uint64) error
}
