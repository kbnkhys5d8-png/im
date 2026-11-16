package handler

import (
	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
)

// 客户端收到消息后的回执
func (h *Handler) recvack(event *eventbus.Event) {
	recvackPacket := event.Frame.(*wkproto.RecvackPacket)
	persist := !recvackPacket.NoPersist // 是否需要持久化
	// 记录消息路径
	event.Track.Record(track.PositionUserRecvack)

	trace.GlobalTrace.Metrics.App().RecvackPacketCountAdd(1)
	trace.GlobalTrace.Metrics.App().RecvackPacketBytesAdd(recvackPacket.GetFrameSize())

	conn := event.Conn
	if !persist || recvackPacket.MessageID <= 0 || recvackPacket.MessageSeq == 0 {
		return
	}

	currMsg := service.RetryManager.AcknowledgeRetry(conn.Uid, conn.NodeId, conn.ConnId, recvackPacket.MessageID, recvackPacket.MessageSeq)
	if seq := acknowledgedCommandSeq(conn, currMsg); seq != 0 {
		err := service.Store.AdvanceCommandSyncCursorOnRecvack(currMsg.Uid, currMsg.ChannelId, currMsg.ChannelType, seq)
		if err != nil {
			h.Error("AdvanceCommandSyncCursorOnRecvack failed", zap.Error(err), zap.String("channelId", currMsg.ChannelId), zap.Uint8("channelType", currMsg.ChannelType), zap.Uint64("messageSeq", seq))
		}
	}

	if !options.G.Logger.TraceOn {
		return
	}
	fields := []zap.Field{
		zap.Int64("messageId", recvackPacket.MessageID),
		zap.Uint64("messageSeq", uint64(recvackPacket.MessageSeq)),
		zap.String("uid", conn.Uid),
		zap.String("deviceId", conn.DeviceId),
		zap.String("deviceFlag", conn.DeviceFlag.String()),
		zap.Int64("connId", conn.ConnId),
	}
	if currMsg == nil {
		h.Trace("收到消息回执", "recvack", fields...)
	} else if currMsg.ChannelType == wkproto.ChannelTypePerson {
		fields = append(fields, zap.String("channelId", currMsg.ChannelId), zap.Uint8("channelType", currMsg.ChannelType))
		h.Trace("收到消息回执...", "recvack", fields...)
	}
}

func acknowledgedCommandSeq(conn *eventbus.Conn, msg *types.RetryMessage) uint64 {
	// 命令标志和游标取自已匹配的服务端下行包；旧客户端无需回传命令标志。
	if msg == nil || msg.RecvPacket == nil || !msg.RecvPacket.SyncOnce ||
		msg.RecvPacket.NoPersist || conn.DeviceLevel != wkproto.DeviceLevelMaster {
		return 0
	}
	return uint64(msg.RecvPacket.MessageSeq)
}
