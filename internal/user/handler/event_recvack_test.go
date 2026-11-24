package handler

import (
	"context"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/eventbus"
	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/internal/service"
	"github.com/WuKongIM/WuKongIM/internal/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/icluster"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
)

func TestRecvackOnlyUsesMatchedDownlinkForCommandCursor(t *testing.T) {
	oldOptions, oldTrace, oldStore, oldRetry := options.G, trace.GlobalTrace, service.Store, service.RetryManager
	options.G = options.New()
	trace.GlobalTrace = trace.New(context.Background(), trace.NewOptions())
	t.Cleanup(func() {
		options.G, trace.GlobalTrace, service.Store, service.RetryManager = oldOptions, oldTrace, oldStore, oldRetry
	})

	tests := []struct {
		name         string
		proto        uint8
		id           int64
		seq          uint32
		ackSync      bool
		ackNoPersist bool
		downlinkSync bool
		master       bool
		duplicate    bool
		wantCalls    int
		wantConsumed bool
		wantCursor   uint64
	}{
		{name: "旧协议v2省略命令标志", proto: 2, id: 10, seq: 19, downlinkSync: true, master: true, duplicate: true, wantCalls: 2, wantConsumed: true, wantCursor: 19},
		{name: "旧协议v3省略命令标志", proto: 3, id: 10, seq: 19, downlinkSync: true, master: true, wantCalls: 1, wantConsumed: true, wantCursor: 19},
		{name: "普通消息回执不改变命令游标", proto: 3, id: 10, seq: 19, ackSync: true, master: true, wantCalls: 1, wantConsumed: true},
		{name: "从设备不能推进命令游标", proto: 3, id: 10, seq: 19, downlinkSync: true, wantCalls: 1, wantConsumed: true},
		{name: "回执序列不匹配", proto: 3, id: 10, seq: 20, downlinkSync: true, master: true, wantCalls: 1},
		{name: "无效消息ID", proto: 3, id: -1, seq: 19, downlinkSync: true, master: true},
		{name: "无效消息序列", proto: 3, id: 10, seq: 0, downlinkSync: true, master: true},
		{name: "不持久化的回执", proto: 3, id: 10, seq: 19, ackNoPersist: true, downlinkSync: true, master: true},
	}
	codec := wkproto.New()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			conn := &eventbus.Conn{Uid: "ack-recipient", NodeId: 2, ConnId: 3, ProtoVersion: tc.proto, SessionId: "server-session"}
			if tc.master {
				conn.DeviceLevel = wkproto.DeviceLevelMaster
			}
			msg := &types.RetryMessage{Uid: conn.Uid, FromNode: conn.NodeId, ConnId: conn.ConnId, MessageId: 10, SessionId: conn.SessionId,
				ChannelId: "receipt-channel", ChannelType: wkproto.ChannelTypeGroup,
				RecvPacket: &wkproto.RecvPacket{Framer: wkproto.Framer{SyncOnce: tc.downlinkSync}, MessageID: 10, MessageSeq: 19}}
			retry := &recvackRetryProbe{pending: msg}
			slot := &recvackSlotProbe{t: t}
			service.RetryManager = retry
			service.Store = store.New(store.NewOptions(store.WithSlot(slot)))
			encoded, err := codec.EncodeFrame(&wkproto.RecvackPacket{
				Framer:    wkproto.Framer{SyncOnce: tc.ackSync, NoPersist: tc.ackNoPersist},
				MessageID: tc.id, MessageSeq: tc.seq,
			}, tc.proto)
			if err != nil {
				t.Fatal(err)
			}
			frame, _, err := codec.DecodeFrame(encoded, tc.proto)
			if err != nil {
				t.Fatal(err)
			}
			handler := &Handler{}
			handler.recvack(&eventbus.Event{Conn: conn, Frame: frame})
			if tc.duplicate {
				handler.recvack(&eventbus.Event{Conn: conn, Frame: frame})
			}
			if retry.calls != tc.wantCalls || (retry.pending == nil) != tc.wantConsumed {
				t.Fatalf("回执确认次数或任务状态错误：调用=%d 已消费=%t", retry.calls, retry.pending == nil)
			}
			if tc.wantCursor == 0 {
				if slot.proposals != 0 {
					t.Fatal("不应提交会话游标")
				}
			} else if slot.proposals != 1 || slot.seq != tc.wantCursor || slot.uid != msg.Uid || slot.channel != msg.ChannelId || slot.channelType != msg.ChannelType {
				t.Fatalf("应只推进一次已发送命令的游标，实际提交次数=%d 游标=%d", slot.proposals, slot.seq)
			}
		})
	}
}

type recvackRetryProbe struct {
	service.RetryMgr
	pending *types.RetryMessage
	calls   int
}

func (p *recvackRetryProbe) AcknowledgeRetry(uid string, nodeId uint64, connId, messageId int64, seq uint32, sessionId string) *types.RetryMessage {
	p.calls++
	msg := p.pending
	if msg == nil || msg.Uid != uid || msg.FromNode != nodeId ||
		msg.ConnId != connId || msg.MessageId != messageId || msg.RecvPacket.MessageSeq != seq || msg.SessionId != sessionId {
		return nil
	}
	p.pending = nil
	return msg
}

type recvackSlotProbe struct {
	icluster.Slot
	t           *testing.T
	proposals   int
	uid         string
	channel     string
	channelType uint8
	seq         uint64
}

func (*recvackSlotProbe) GetSlotId(key string) uint32 {
	if key == "ack-recipient" {
		return 1
	}
	return 2
}

func (s *recvackSlotProbe) Propose(slotID uint32, data []byte) (*rafttypes.ProposeResp, error) {
	s.t.Helper()
	if slotID != 1 {
		s.t.Fatalf("群命令回执应写入用户槽 1，实际为 %d", slotID)
	}
	s.proposals++
	cmd := &store.CMD{}
	if err := cmd.Unmarshal(data); err != nil {
		s.t.Fatal(err)
	}
	if cmd.CmdType != store.CMDAdvanceCommandSyncCursorOnRecvack {
		s.t.Fatalf("实时回执不应以补拉页的方式推进：%s", cmd.CmdType)
	}
	uid, channel, channelType, seq, err := cmd.DecodeCMDUpdateConversationIfSeqGreater()
	if err != nil {
		s.t.Fatal(err)
	}
	s.uid, s.channel, s.channelType, s.seq = uid, channel, channelType, seq
	return nil, nil
}
