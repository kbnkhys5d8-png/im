package server

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/WuKongIM/WuKongIM/internal/options"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wknet"
	wkproto "github.com/WuKongIM/WuKongIMGoProto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type firstFrameLog struct {
	wklog.Log
	messages []string
	fields   [][]zap.Field
}

func (l *firstFrameLog) Warn(message string, fields ...zap.Field) {
	l.messages = append(l.messages, message)
	l.fields = append(l.fields, fields)
}

type firstFrameConn struct {
	wknet.Conn
	closed bool
}

func (c *firstFrameConn) Close() error {
	c.closed = true
	return nil
}

type firstFrameProtocol struct {
	wkproto.Protocol
	decodeErr error
	calls     int
}

func (p *firstFrameProtocol) DecodeFrame([]byte, uint8) (wkproto.Frame, int, error) {
	p.calls++
	return nil, 0, p.decodeErr
}

func TestUnauthenticatedFirstFrameLogsExcludePayload(t *testing.T) {
	const payload = "synthetic-private-frame"
	decodeErr := errors.New("synthetic decoder error: " + payload)
	tests := []struct {
		name        string
		frameType   wkproto.FrameType
		decodeErr   error
		errorClass  string
		decodeCalls int
		wantErr     error
	}{
		{name: "unexpected_frame", frameType: wkproto.SEND, errorClass: "unexpected_frame_type"},
		{name: "decode_error", frameType: wkproto.CONNECT, decodeErr: decodeErr, errorClass: "decode_failed", decodeCalls: 1, wantErr: decodeErr},
		{name: "nil_frame", frameType: wkproto.CONNECT, errorClass: "nil_frame", decodeCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger := &firstFrameLog{}
			protocol := &firstFrameProtocol{decodeErr: test.decodeErr}
			opts := options.New()
			opts.Proto = protocol
			server := &Server{opts: opts, Log: logger}
			conn := &firstFrameConn{}
			frame := append([]byte{byte(test.frameType << 4), byte(len(payload))}, []byte(payload)...)
			ctx, consumed, err := server.handleUnauthenticatedConn(conn, frame, false)
			if ctx != nil || consumed != 0 || !conn.closed || protocol.calls != test.decodeCalls {
				t.Fatal("异常首帧的连接处理行为发生变化")
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatal("原解码错误必须继续返回给调用方")
			}
			if test.name == "unexpected_frame" && err != nil {
				t.Fatal("非CONNECT首帧必须保留原返回行为")
			}
			if test.name == "nil_frame" && (err == nil || err.Error() != "decoded nil frame") {
				t.Fatal("空帧必须保留原错误")
			}
			if len(logger.messages) != 1 || len(logger.fields) != 1 {
				t.Fatal("异常首帧必须保留一条告警")
			}
			encoded := zapcore.NewMapObjectEncoder()
			for _, field := range logger.fields[0] {
				field.AddTo(encoded)
			}
			fields := encoded.Fields
			if fields["error_class"] != test.errorClass || fields["frameType"] != test.frameType.String() || fields["frame_bytes"] != int64(len(frame)) {
				t.Fatal("告警必须保留固定类别、帧类型和字节数")
			}
			if len(fields) != 3 {
				t.Fatal("首帧日志不能包含白名单外的字段")
			}
			data, marshalErr := json.Marshal(fields)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			if strings.Contains(string(data), payload) || strings.Contains(logger.messages[0], payload) {
				t.Fatal("首帧日志不能包含合成私密正文")
			}
		})
	}
}

func TestUnauthenticatedPartialFirstFrameRemainsOpen(t *testing.T) {
	logger := &firstFrameLog{}
	protocol := &firstFrameProtocol{}
	opts := options.New()
	opts.Proto = protocol
	server := &Server{opts: opts, Log: logger}
	conn := &firstFrameConn{}
	ctx, consumed, err := server.handleUnauthenticatedConn(conn, []byte{byte(wkproto.CONNECT << 4), 20, 1}, false)
	if ctx != nil || consumed != 0 || err != nil || conn.closed || protocol.calls != 0 || len(logger.messages) != 0 {
		t.Fatal("不完整首帧必须继续等待，不能提前解码、断开或记告警")
	}
}
