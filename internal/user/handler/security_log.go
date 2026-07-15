package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"go.uber.org/zap"
)

// safeConnectionLogFields deliberately excludes device IDs, tokens and
// connection encryption material.
func safeConnectionLogFields(conn *eventbus.Conn) []zap.Field {
	if conn == nil {
		return []zap.Field{zap.Bool("connNil", true)}
	}
	return []zap.Field{
		zap.String("uid", conn.Uid),
		zap.Uint64("nodeId", conn.NodeId),
		zap.Int64("connId", conn.ConnId),
		zap.String("deviceFlag", conn.DeviceFlag.String()),
		zap.String("deviceLevel", conn.DeviceLevel.String()),
	}
}
