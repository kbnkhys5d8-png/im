package handler

import (
	"context"
	"errors"
	"strings"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/ingress"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/track"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/internal/types/pluginproto"
	"github.com/WuKongIM/WuKongIM/pkg/messagepayload"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"github.com/valyala/bytebufferpool"
	"go.uber.org/zap"
)

// onSend 收到发送消息
func (h *Handler) onSend(ctx *eventbus.UserContext) {
	for _, event := range ctx.Events {
		// 撤销与消息入场共用 UID 队列；关闭 RPC 尚在途时也不能再接受旧会话。
		current := eventbus.User.ConnById(event.Conn.Uid, event.Conn.NodeId, event.Conn.ConnId)
		if !current.SameSession(event.Conn) {
			var err error
			current, err = h.restoreSourceConn(ctx, event.Conn)
			if err != nil {
				h.closeUnrestoredConn(event, err)
				continue
			}
		}
		if !current.SameSession(event.Conn) || current.Revoked.Load() {
			continue
		}
		event.Conn = current
		frameType := event.Frame.GetFrameType()
		switch frameType {
		case wkproto.SEND:
			h.handleOnSend(event)
		case wkproto.RECVACK:
			h.recvack(event)
		case wkproto.PING:
			h.ping(event)
		}
	}
}

// 新 leader 的索引可能缺失或停留在旧代际；仅恢复源节点仍持有且凭据有效的精确会话。
func (h *Handler) restoreSourceConn(ctx *eventbus.UserContext, conn *eventbus.Conn) (*eventbus.Conn, error) {
	if ctx.AddConn == nil || conn == nil || !conn.Auth || conn.SessionId == "" || conn.Revoked.Load() {
		return nil, nil
	}
	if conn.Uid == options.G.ManagerUID {
		if options.G.ManagerTokenOn && conn.AuthTokenHash != eventbus.TokenFingerprint(options.G.ManagerToken) {
			return nil, nil
		}
	} else if options.G.TokenAuthOn && !options.G.IsVisitors(conn.Uid) {
		device, err := service.Store.GetDevice(conn.Uid, conn.DeviceFlag)
		if err != nil {
			return nil, err
		}
		// 补恢复必须验证原会话等级，不能把旧会话直接改成当前权限后重新登记。
		if device.Token == "" || conn.AuthTokenHash != eventbus.TokenFingerprint(device.Token) ||
			conn.DeviceLevel != wkproto.DeviceLevel(device.DeviceLevel) {
			return nil, nil
		}
	}
	channel, err := service.Store.GetChannel(conn.Uid, wkproto.ChannelTypePerson)
	if err != nil {
		return nil, err
	}
	if channel.Ban {
		return nil, nil
	}
	var sources []*eventbus.Conn
	if options.G.IsLocalNode(conn.NodeId) {
		sources = ingress.LocalUserConns(conn.Uid)
	} else {
		sources, err = ingress.NewClient().UserConns(conn.NodeId, conn.Uid)
		if err != nil {
			return nil, err
		}
	}
	for _, source := range sources {
		if source.Auth && source.SameSession(conn) && source.AuthTokenHash == conn.AuthTokenHash && source.DeviceLevel == conn.DeviceLevel {
			if !options.G.IsLocalNode(h.userLeaderNodeId(conn.Uid)) {
				return nil, errors.New("恢复会话时认证节点已变化")
			}
			ctx.AddConn(conn)
			return conn, nil
		}
	}
	return nil, nil
}

// 临时恢复失败不确认发送成功；沿原关闭链路处理精确会话，旧客户端无需增加协议字段。
func (h *Handler) closeUnrestoredConn(event *eventbus.Event, err error) {
	h.Warn("恢复会话失败，关闭原连接等待重连", zap.Error(err), zap.String("uid", event.Conn.Uid))
	var frame wkproto.Frame
	if send, ok := event.Frame.(*wkproto.SendPacket); ok {
		frame = &wkproto.SendackPacket{Framer: send.Framer, MessageID: event.MessageId,
			ClientSeq: send.ClientSeq, ClientMsgNo: send.ClientMsgNo, ReasonCode: wkproto.ReasonSystemError}
	}
	// 回执随关闭事件传递，避免按类型分组时先关闭、后写回执；不发送会清登录态的踢号通知。
	eventbus.User.AddEvent(event.Conn.Uid, &eventbus.Event{Type: eventbus.EventConnClose, Conn: event.Conn,
		Frame: frame, ReqId: event.ReqId, SourceNodeId: options.G.Cluster.NodeId})
	eventbus.User.Advance(event.Conn.Uid)
}

func (h *Handler) handleOnSend(event *eventbus.Event) {

	// 记录消息路径
	event.Track.Record(track.PositionUserOnSend)

	conn := event.Conn
	from := conn.Uid
	sendPacket := event.Frame.(*wkproto.SendPacket)
	channelId := sendPacket.ChannelID
	channelType := sendPacket.ChannelType
	fakeChannelId, validChannelID := resolveSendChannelID(channelId, channelType, conn.Uid)
	if !validChannelID {
		sendack := &wkproto.SendackPacket{
			Framer:      sendPacket.Framer,
			MessageID:   event.MessageId,
			ClientSeq:   sendPacket.ClientSeq,
			ClientMsgNo: sendPacket.ClientMsgNo,
			ReasonCode:  wkproto.ReasonChannelIDError,
		}
		eventbus.User.ConnWrite(event.ReqId, conn, sendack)
		return
	}

	if options.G.Logger.TraceOn {
		h.Trace("用户发送消息...",
			"onSend",
			zap.Int64("messageId", event.MessageId),
			zap.Uint64("messageSeq", event.MessageSeq),
			zap.String("from", from),
			zap.String("deviceId", event.Conn.DeviceId),
			zap.String("deviceFlag", event.Conn.DeviceFlag.String()),
			zap.Int64("connId", event.Conn.ConnId),
			zap.String("channelId", fakeChannelId),
			zap.Uint8("channelType", channelType),
		)
	}

	reasonCode, err := h.checkGlobalSendPermission(from)
	if err != nil {
		h.Error("checkGlobalSendPermission error", zap.Error(err), zap.String("uid", from))
		sendack := &wkproto.SendackPacket{
			Framer:      sendPacket.Framer,
			MessageID:   event.MessageId,
			ClientSeq:   sendPacket.ClientSeq,
			ClientMsgNo: sendPacket.ClientMsgNo,
			ReasonCode:  wkproto.ReasonSystemError,
		}
		eventbus.User.ConnWrite(event.ReqId, conn, sendack)
		return
	}

	if reasonCode != wkproto.ReasonSuccess {
		h.Warn("checkGlobalSendPermission failed", zap.String("uid", from), zap.String("reasonCode", reasonCode.String()))
		sendack := &wkproto.SendackPacket{
			Framer:      sendPacket.Framer,
			MessageID:   event.MessageId,
			ClientSeq:   sendPacket.ClientSeq,
			ClientMsgNo: sendPacket.ClientMsgNo,
			ReasonCode:  reasonCode,
		}
		eventbus.User.ConnWrite(event.ReqId, conn, sendack)
		return
	}

	// 根据配置决定是否解密消息
	if !options.G.DisableEncryption && !conn.IsJsonRpc {
		newPayload, err := h.decryptPayload(sendPacket, conn)
		if err != nil {
			h.Error("handleOnSend: Failed to decrypt payload！", zap.Error(err), zap.String("uid", conn.Uid), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
			sendack := &wkproto.SendackPacket{
				Framer:      sendPacket.Framer,
				MessageID:   event.MessageId,
				ClientSeq:   sendPacket.ClientSeq,
				ClientMsgNo: sendPacket.ClientMsgNo,
				ReasonCode:  wkproto.ReasonPayloadDecodeError,
			}
			eventbus.User.ConnWrite(event.ReqId, conn, sendack)
			return
		}
		sendPacket.Payload = newPayload // 使用解密后的 Payload
	} else {
		// 如果禁用了加密，则直接使用原始 Payload，不做任何操作
		// sendPacket.Payload 保持不变
	}
	// TCP/WSS 必须按解密后的正文校验，复用现有失败 ACK，不改变客户端协议。
	if !validateSendText(event, sendPacket) {
		return
	}
	if !h.validateClientWalletContent(event, sendPacket) {
		return
	}

	// 调用插件
	reason, err := h.pluginInvokeSend(sendPacket, event)
	if err != nil || reason != wkproto.ReasonSuccess {
		h.Info("handleOnSend: plugin return error reason", zap.Error(err), zap.Uint8("reason", uint8(reason)), zap.String("uid", conn.Uid), zap.String("channelId", channelId), zap.Uint8("channelType", channelType))
		sendack := &wkproto.SendackPacket{
			Framer:      sendPacket.Framer,
			MessageID:   event.MessageId,
			ClientSeq:   sendPacket.ClientSeq,
			ClientMsgNo: sendPacket.ClientMsgNo,
			ReasonCode:  reason,
		}
		eventbus.User.ConnWrite(event.ReqId, conn, sendack)
		return
	}
	// 插件可能替换正文，最终入队内容必须再次验证。
	if !validateSendText(event, sendPacket) {
		return
	}
	// 插件改写也不能把客户端消息变成金融卡片；服务端钱包走独立的管理接口，不经过此入口。
	if !h.validateClientWalletContent(event, sendPacket) {
		return
	}

	trace.GlobalTrace.Metrics.App().SendPacketCountAdd(1)
	trace.GlobalTrace.Metrics.App().SendPacketBytesAdd(sendPacket.GetFrameSize())
	// 添加消息到频道
	eventbus.Channel.SendMessage(fakeChannelId, channelType, &eventbus.Event{
		Type:      eventbus.EventChannelOnSend,
		Conn:      conn,
		Frame:     sendPacket,
		MessageId: event.MessageId,
		Track:     event.Track,
		ReqId:     event.ReqId,
	})
	// 推进
	eventbus.Channel.Advance(fakeChannelId, channelType)

}

func (h *Handler) validateClientWalletContent(event *eventbus.Event, packet *wkproto.SendPacket) bool {
	contentType, valid := clientMessageContentType(packet.Payload)
	if valid && contentType != 9 && contentType != 10 {
		return true
	}
	h.Warn("client attempted to send reserved or invalid wallet content",
		zap.String("uid", event.Conn.Uid), zap.String("channelId", packet.ChannelID),
		zap.Uint8("channelType", packet.ChannelType), zap.Int64("contentType", contentType),
	)
	eventbus.User.ConnWrite(event.ReqId, event.Conn, &wkproto.SendackPacket{
		Framer: packet.Framer, MessageID: event.MessageId, ClientSeq: packet.ClientSeq,
		ClientMsgNo: packet.ClientMsgNo, ReasonCode: wkproto.ReasonNotAllowSend,
	})
	return false
}

func validateSendText(event *eventbus.Event, packet *wkproto.SendPacket) bool {
	if messagepayload.ValidateText(packet.Payload) == nil {
		return true
	}
	eventbus.User.ConnWrite(event.ReqId, event.Conn, &wkproto.SendackPacket{
		Framer: packet.Framer, MessageID: event.MessageId, ClientSeq: packet.ClientSeq,
		ClientMsgNo: packet.ClientMsgNo, ReasonCode: wkproto.ReasonPayloadDecodeError,
	})
	return false
}

func resolveSendChannelID(channelID string, channelType uint8, uid string) (string, bool) {
	if strings.Contains(channelID, "&") {
		return "", false
	}

	resolvedChannelID := channelID
	if channelType == wkproto.ChannelTypePerson {
		resolvedChannelID = options.GetFakeChannelIDWith(channelID, uid)
	} else if channelType == wkproto.ChannelTypeAgent {
		resolvedChannelID = options.GetAgentChannelIDWith(uid, channelID)
	}
	if strings.Contains(resolvedChannelID, "&") {
		return "", false
	}
	return resolvedChannelID, true
}

// 检查发送者全局权限
func (h *Handler) checkGlobalSendPermission(from string) (wkproto.ReasonCode, error) {
	channelInfo, err := service.Store.GetChannel(from, wkproto.ChannelTypePerson)
	if err != nil {
		return wkproto.ReasonSystemError, err
	}
	if channelInfo.SendBan {
		return wkproto.ReasonSendBan, nil
	}
	return wkproto.ReasonSuccess, nil
}

func (h *Handler) pluginInvokeSend(sendPacket *wkproto.SendPacket, event *eventbus.Event) (wkproto.ReasonCode, error) {
	plugins := service.PluginManager.Plugins(types.PluginSend)

	if len(plugins) == 0 {
		return wkproto.ReasonSuccess, nil
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), options.G.Plugin.Timeout)
	defer cancel()

	conn := &pluginproto.Conn{
		Uid:         event.Conn.Uid,
		ConnId:      event.Conn.ConnId,
		DeviceId:    event.Conn.DeviceId,
		DeviceFlag:  uint32(event.Conn.DeviceFlag),
		DeviceLevel: uint32(event.Conn.DeviceLevel),
	}

	pluginPacket := &pluginproto.SendPacket{
		FromUid:     event.Conn.Uid,
		ChannelId:   sendPacket.ChannelID,
		ChannelType: uint32(sendPacket.ChannelType),
		Payload:     sendPacket.Payload,
		Conn:        conn,
		Reason:      uint32(wkproto.ReasonSuccess),
	}
	for _, pg := range plugins {
		result, err := pg.Send(timeoutCtx, pluginPacket)
		if err != nil {
			h.Error("pluginInvokeSend: Failed to invoke plugin！", zap.Error(err), zap.String("plugin", pg.GetNo()))
			return wkproto.ReasonSystemError, err
		}
		if result == nil {
			continue
		}
		if result.Reason == 0 {
			result.Reason = uint32(wkproto.ReasonSuccess)
		}
		pluginPacket = result
	}

	// 使用插件处理后的消息
	sendPacket.Payload = pluginPacket.Payload
	return wkproto.ReasonCode(pluginPacket.Reason), nil
}

// decode payload
func (h *Handler) decryptPayload(sendPacket *wkproto.SendPacket, conn *eventbus.Conn) ([]byte, error) {

	aesKey, aesIV := conn.AesKey, conn.AesIV
	vail, err := h.sendPacketIsVail(sendPacket, conn)
	if err != nil {
		return nil, err
	}
	if !vail {
		return nil, errors.New("sendPacket is illegal！")
	}
	// decode payload
	decodePayload, err := wkutil.AesDecryptPkcs7Base64(sendPacket.Payload, aesKey, aesIV)
	if err != nil {
		h.Error("Failed to decode payload！", zap.Error(err))
		return nil, err
	}

	return decodePayload, nil
}

// send packet is vail
func (h *Handler) sendPacketIsVail(sendPacket *wkproto.SendPacket, conn *eventbus.Conn) (bool, error) {
	aesKey, aesIV := conn.AesKey, conn.AesIV
	signStr := sendPacket.VerityString()

	signBuff := bytebufferpool.Get()
	_, _ = signBuff.WriteString(signStr)

	defer bytebufferpool.Put(signBuff)

	actMsgKey, err := wkutil.AesEncryptPkcs7Base64(signBuff.Bytes(), aesKey, aesIV)
	if err != nil {
		fields := append([]zap.Field{zap.Error(err)}, safeConnectionLogFields(conn)...)
		h.Error("msgKey is illegal！", fields...)
		return false, err
	}
	actMsgKeyStr := sendPacket.MsgKey
	exceptMsgKey := wkutil.MD5Bytes(actMsgKey)
	if actMsgKeyStr != exceptMsgKey {
		h.Error("msgKey is illegal！", safeConnectionLogFields(conn)...)
		return false, errors.New("msgKey is illegal！")
	}
	return true, nil
}
