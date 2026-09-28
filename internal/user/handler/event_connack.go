package handler

import (
	"context"
	"time"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

func (h *Handler) connack(ctx *eventbus.UserContext) {
	for _, event := range ctx.Events {
		conn := event.Conn
		frame := event.Frame
		uid := conn.Uid
		if conn.NodeId == 0 {
			h.Error("processConnack: from node is 0", zap.String("uid", uid))
			return
		}
		if frame == nil {
			h.Error("processConnack: frame is nil", zap.String("uid", uid))
			return
		}
		connack := frame.(*wkproto.ConnackPacket)
		var realConn wknet.Conn
		if options.G.IsLocalNode(conn.NodeId) {
			realConn = service.ConnManager.GetConn(conn.ConnId)
			// 目录可能尚未登记新连接，覆盖真实上下文之前也必须核对会话代次。
			if realConn == nil {
				if conn.SessionId != "" {
					continue
				}
			} else if current, ok := realConn.Context().(*eventbus.Conn); ok && current != nil {
				if current.SessionId != conn.SessionId {
					continue
				}
				// 接入代次只取 owner 原始上下文，不信任认证节点回传的数据。
				conn.AdmissionVersion = current.AdmissionVersion
			} else if conn.SessionId != "" {
				continue
			}
		}
		if connack.ReasonCode == wkproto.ReasonSuccess {
			// 设置连接最大空闲时间
			if realConn != nil {
				realConn.SetMaxIdle(options.G.ConnIdleTime)
				realConn.SetContext(conn)
			}
			connack.NodeId = options.G.Cluster.NodeId
			// 更新连接
			eventbus.User.UpdateConn(conn)
			if recovery, ok := service.ConnRecovery.(service.IAuthenticatedConnRecovery); ok &&
				realConn != nil && conn.SessionId != "" && event.SourceNodeId != 0 {
				// 先发布 owner 的真实认证状态，再确认换主后的目录；确认前不回成功。
				confirmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := recovery.ConfirmAuthenticated(confirmCtx, conn, event.SourceNodeId)
				cancel()
				if err != nil {
					h.Warn("迟到认证连接恢复失败", zap.String("uid", uid), zap.Error(err))
					current, matches := realConn.Context().(*eventbus.Conn)
					if matches && current.SameSession(conn) {
						// 只关闭捕获到的这次 socket，不按可复用 ConnId 关闭其他会话。
						if closeErr := realConn.Close(); closeErr != nil {
							h.Warn("关闭未确认认证连接失败", zap.String("uid", uid), zap.Error(closeErr))
						}
					}
					continue
				}
			}
		}
		if realConn != nil && conn.SessionId != "" {
			current, matches := realConn.Context().(*eventbus.Conn)
			if service.ConnManager.GetConn(conn.ConnId) != realConn || !matches || !current.SameSession(conn) {
				// 确认 RPC 期间发生换代，不给已失效的 socket 排队回执。
				continue
			}
		}
		eventbus.User.ConnWrite(event.ReqId, conn, connack)
	}

}
