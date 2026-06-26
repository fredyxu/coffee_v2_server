package log

import (
	"bytes"
	"coffee_server/config"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func init() {
	var err error
	var configZap zap.Config

	// 根据配置模式选择不同的日志配置
	if config.Mode == "debug" {
		// --- 关键改动: 使用 JSON 格式编码器，并配置美化打印 ---
		configZap = zap.NewDevelopmentConfig()
		configZap.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		configZap.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		configZap.EncoderConfig.CallerKey = "caller"

		// 覆盖默认编码器，使用 JSON 格式
		configZap.Encoding = "json"

		// 生产环境
	} else {
		configZap = zap.NewProductionConfig()
		configZap.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		configZap.DisableCaller = true
		configZap.Level.SetLevel(zap.InfoLevel)
	}

	logger, err = configZap.Build()
	if err != nil {
		fmt.Printf("无法初始化日志： %v\n", err)
		os.Exit(1)
	}
	zap.ReplaceGlobals(logger)
}

// D 打印详细的调试信息，支持多种类型，并包含调用栈信息。
// 该函数仅用于开发和调试，在生产环境中没有任何开销。
func D(args ...interface{}) {
	if !zap.L().Core().Enabled(zap.DebugLevel) {
		return
	}

	pc, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "???"
		line = 0
	}
	funcName := "???"
	if f := runtime.FuncForPC(pc); f != nil {
		funcName = f.Name()
		if lastSlash := strings.LastIndexByte(funcName, '/'); lastSlash >= 0 {
			funcName = funcName[lastSlash+1:]
		}
	}

	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("\n%s:%d -> %s\n", file, line, funcName))
	buf.WriteString("----------------------------------------------------------------------\n")

	for _, arg := range args {
		if reflect.TypeOf(arg).Kind() == reflect.Struct || reflect.TypeOf(arg).Kind() == reflect.Map {
			jsonData, err := json.MarshalIndent(arg, "", "  ")
			if err == nil {
				buf.Write(jsonData)
				buf.WriteString("\n")
				continue
			}
		}

		buf.WriteString(fmt.Sprintf("%+v\n", arg))
	}
	buf.WriteString("----------------------------------------------------------------------")

	// 将所有内容作为一个单独的 zap.String 字段来记录
	// 由于我们将开发环境的编码器设为 JSON，它会正确打印这个字符串
	zap.L().Debug("调试信息", zap.String("details", buf.String()))
}

// Debug 记录调试级别的日志
func Debug(msg string, fields ...zap.Field) {
	zap.L().Debug(msg, fields...)
}

// Info 记录信息级别的日志
func Info(msg string, fields ...zap.Field) {
	zap.L().Info(msg, fields...)
}

// Warn 记录警告级别的日志
func Warn(msg string, fields ...zap.Field) {
	zap.L().Warn(msg, fields...)
}

// Error 记录错误级别的日志
func Error(msg string, fields ...zap.Field) {
	zap.L().Error(msg, fields...)
}

// Fatal 记录致命错误，并退出程序
func Fatal(msg string, fields ...zap.Field) {
	zap.L().Fatal(msg, fields...)
}

// Sync 刷新日志缓冲，确保日志被写入
func Sync() {
	_ = zap.L().Sync()
}
