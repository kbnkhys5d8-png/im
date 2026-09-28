package wklog

import (
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// 日志实例与配置整体发布，发布后只读，避免并发调用看到初始化到一半的状态。
type loggerState struct {
	logger      *zap.Logger
	traceLogger *zap.Logger
	errorLogger *zap.Logger
	warnLogger  *zap.Logger
	panicLogger *zap.Logger
	focusLogger *zap.Logger
	opts        Options
}

var activeLoggers atomic.Pointer[loggerState]
var configureMu sync.Mutex

func Configure(op *Options) {
	configureMu.Lock()
	defer configureMu.Unlock()
	configureLocked(op)
}

func currentLoggers() *loggerState {
	if state := activeLoggers.Load(); state != nil {
		return state
	}
	configureMu.Lock()
	defer configureMu.Unlock()
	// 与显式配置共用锁并再次检查，默认初始化不能覆盖已生效的业务配置。
	if activeLoggers.Load() == nil {
		configureLocked(NewOptions())
	}
	return activeLoggers.Load()
}

func configureLocked(op *Options) {
	state := &loggerState{opts: *op}
	opts := &state.opts
	atom := zap.NewAtomicLevelAt(opts.Level)

	loggerOpts := make([]zap.Option, 0)
	if opts.LineNum {
		loggerOpts = append(loggerOpts, zap.AddCaller(), zap.AddCallerSkip(2))
	}

	writers := make([]zapcore.WriteSyncer, 0)
	if !opts.NoStdout {
		writers = append(writers, zapcore.AddSync(os.Stdout))
	}

	// ====================== info ==========================
	infoWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path.Join(opts.LogDir, "info.log"),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
	})
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(newEncoderConfig()),
		zapcore.NewMultiWriteSyncer(append(writers, zapcore.AddSync(infoWriter))...),
		atom,
	)
	state.logger = zap.New(core, loggerOpts...)

	// ====================== trace ==========================
	traceWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path.Join(opts.LogDir, "trace.log"),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
	})
	core = zapcore.NewCore(
		zapcore.NewJSONEncoder(newEncoderConfig()),
		zapcore.NewMultiWriteSyncer(append(writers, zapcore.AddSync(traceWriter))...),
		atom,
	)
	state.traceLogger = zap.New(core, loggerOpts...)

	// ====================== error ==========================
	errorWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path.Join(opts.LogDir, "error.log"),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
	})
	core = zapcore.NewCore(
		zapcore.NewJSONEncoder(newEncoderConfig()),
		zapcore.NewMultiWriteSyncer(append(writers, zapcore.AddSync(errorWriter))...),
		zap.ErrorLevel,
	)
	state.errorLogger = zap.New(core, loggerOpts...)

	// ====================== warn ==========================
	warnWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path.Join(opts.LogDir, "warn.log"),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
	})
	core = zapcore.NewCore(
		zapcore.NewJSONEncoder(newEncoderConfig()),
		zapcore.NewMultiWriteSyncer(append(writers, zapcore.AddSync(warnWriter))...),
		zap.WarnLevel,
	)
	state.warnLogger = zap.New(core, loggerOpts...)

	// ====================== panic ==========================
	panicWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path.Join(opts.LogDir, "panic.log"),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
	})
	core = zapcore.NewCore(
		zapcore.NewJSONEncoder(newEncoderConfig()),
		zapcore.NewMultiWriteSyncer(append(writers, zapcore.AddSync(panicWriter))...),
		zap.PanicLevel,
	)
	state.panicLogger = zap.New(core, append(loggerOpts, zap.AddStacktrace(zapcore.PanicLevel))...)

	// ====================== focus ==========================
	focusWriter := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path.Join(opts.LogDir, "focus.log"),
		MaxSize:    500, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
	})
	core = zapcore.NewCore(
		zapcore.NewJSONEncoder(newEncoderConfig()),
		zapcore.NewMultiWriteSyncer(append(writers, zapcore.AddSync(focusWriter))...),
		zap.InfoLevel,
	)
	state.focusLogger = zap.New(core, loggerOpts...)
	activeLoggers.Store(state)

}

func Level() zapcore.Level {

	return currentLoggers().opts.Level
}

func newEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:       "time",
		LevelKey:      "level",
		NameKey:       "logger",
		CallerKey:     "linenum",
		MessageKey:    "msg",
		StacktraceKey: "stacktrace",
		LineEnding:    zapcore.DefaultLineEnding,
		EncodeLevel:   zapcore.LowercaseLevelEncoder, // 小写编码器
		EncodeCaller:  zapcore.FullCallerEncoder,     // 全路径编码器
		EncodeName:    zapcore.FullNameEncoder,
		EncodeTime: func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendString(t.Format("2006-01-02T15:04:05.999999999-07:00"))
		},
		EncodeDuration: func(d time.Duration, enc zapcore.PrimitiveArrayEncoder) {
			enc.AppendInt64(int64(d) / 1000000)
		},
	}
}

// func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
// 	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
// }

// Info Info
func Info(msg string, fields ...zap.Field) {

	currentLoggers().logger.Info(msg, fields...)

}

// Trace Trace
func Trace(msg string, fields ...zap.Field) {

	currentLoggers().traceLogger.Info(msg, fields...)

}

// Debug Debug
func Debug(msg string, fields ...zap.Field) {

	currentLoggers().logger.Debug(msg, fields...)

}

// Error Error
func Error(msg string, fields ...zap.Field) {

	currentLoggers().errorLogger.Error(msg, fields...)

}

func Fatal(msg string, fields ...zap.Field) {

	currentLoggers().panicLogger.Fatal(msg, fields...)
}
func Panic(msg string, fields ...zap.Field) {

	currentLoggers().panicLogger.Panic(msg, fields...)
}

// Warn Warn
func Warn(msg string, fields ...zap.Field) {

	currentLoggers().warnLogger.Warn(msg, fields...)
}

func Foucs(msg string, fields ...zap.Field) {

	currentLoggers().focusLogger.Info(msg, fields...)
}

func Sync() error {
	state := currentLoggers()
	err := state.panicLogger.Sync()
	if err != nil {
		fmt.Println("panicLogger sync error", err)
	}
	err = state.errorLogger.Sync()
	if err != nil {
		fmt.Println("errorLogger sync error", err)
	}
	err = state.warnLogger.Sync()
	if err != nil {
		fmt.Println("warnLogger sync error", err)
	}
	err = state.logger.Sync()
	if err != nil {
		fmt.Println("logger sync error", err)
	}
	return nil
}

// Log Log
type Log interface {
	Info(msg string, fields ...zap.Field)
	MessageTrace(msg string, clientMsgNo string, operationName string, fields ...zap.Field)
	Trace(msg string, action string, fields ...zap.Field)
	Debug(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Fatal(msg string, fields ...zap.Field)
	Panic(msg string, fields ...zap.Field)
	Foucs(msg string, fields ...zap.Field)
}

// WKLog TLog
type WKLog struct {
	prefix string // 日志前缀
}

// NewWKLog NewWKLog
func NewWKLog(prefix string) *WKLog {

	return &WKLog{prefix: prefix}
}

// Info Info
func (t *WKLog) Info(msg string, fields ...zap.Field) {
	var b strings.Builder
	b.WriteString("【")
	b.WriteString(t.prefix)
	b.WriteString("】")
	b.WriteString(msg)
	Info(b.String(), fields...)
}

// Trace Trace
func (t *WKLog) Trace(msg string, action string, fields ...zap.Field) {
	state := currentLoggers()
	if !state.opts.TraceOn {
		return
	}

	var b strings.Builder
	b.WriteString("【")
	b.WriteString(t.prefix)
	b.WriteString("】")
	b.WriteString(msg)
	if len(fields) == 0 {
		state.trace(b.String(), zap.Int("trace", 1), zap.String("action", action))
	} else {
		fields = append(fields, zap.Int("trace", 1), zap.String("action", action))
		state.trace(b.String(), fields...)
	}
}

func (t *WKLog) MessageTrace(msg string, no string, action string, fields ...zap.Field) {

	state := currentLoggers()
	if !state.opts.TraceOn {
		return
	}

	var b strings.Builder
	b.WriteString("【")
	b.WriteString(t.prefix)
	b.WriteString("】")
	b.WriteString(msg)
	if len(fields) == 0 {
		state.trace(b.String(), zap.Int("trace", 1), zap.String("no", no), zap.String("action", action))
	} else {
		fields = append(fields, zap.Int("trace", 1), zap.String("no", no), zap.String("action", action))
		state.trace(b.String(), fields...)
	}

}

// 保持原包级 Trace 的调用深度，且开关检查与写入使用同一份配置快照。
func (s *loggerState) trace(msg string, fields ...zap.Field) {
	s.traceLogger.Info(msg, fields...)
}

// Debug Debug
func (t *WKLog) Debug(msg string, fields ...zap.Field) {
	var b strings.Builder
	b.WriteString("【")
	b.WriteString(t.prefix)
	b.WriteString("】")
	b.WriteString(msg)
	Debug(b.String(), fields...)
}

// Error Error
func (t *WKLog) Error(msg string, fields ...zap.Field) {
	var b strings.Builder
	b.WriteString("【")
	b.WriteString(t.prefix)
	b.WriteString("】")
	b.WriteString(msg)
	Error(b.String(), fields...)
}

// Warn Warn
func (t *WKLog) Warn(msg string, fields ...zap.Field) {
	var b strings.Builder
	b.WriteString("【")
	b.WriteString(t.prefix)
	b.WriteString("】")
	b.WriteString(msg)
	Warn(b.String(), fields...)
}

func (t *WKLog) Fatal(msg string, fields ...zap.Field) {
	var b strings.Builder
	b.WriteString("【")
	b.WriteString(t.prefix)
	b.WriteString("】")
	b.WriteString(msg)
	Fatal(b.String(), fields...)
}
func (t *WKLog) Panic(msg string, fields ...zap.Field) {
	var b strings.Builder
	b.WriteString("【")
	b.WriteString(t.prefix)
	b.WriteString("】")
	b.WriteString(msg)
	Panic(b.String(), fields...)
}

func (t *WKLog) Foucs(msg string, fields ...zap.Field) {
	var b strings.Builder
	b.WriteString("【")
	b.WriteString(t.prefix)
	b.WriteString("】")
	b.WriteString(msg)
	Foucs(b.String(), fields...)
}
