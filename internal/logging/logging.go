package logging

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func New(json bool) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	if !json {
		cfg.Encoding = "console"
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}
	return cfg.Build(zap.AddCaller(), zap.AddCallerSkip(1))
}

func String(key, value string) zap.Field      { return zap.String(key, value) }
func Int64(key string, value int64) zap.Field { return zap.Int64(key, value) }
func Bool(key string, value bool) zap.Field   { return zap.Bool(key, value) }
func Err(err error) zap.Field                 { return zap.Error(err) }
