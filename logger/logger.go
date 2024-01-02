package logger

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func Setup(minimumLogLevel, serviceName string) {
	var level zerolog.Level
	switch minimumLogLevel {
	case "debug":
		level = zerolog.DebugLevel
	case "info":
		level = zerolog.InfoLevel
	case "error":
		level = zerolog.ErrorLevel
	default:
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	// Identify application with logger property
	log.Logger = log.With().Str("service", "manager").Logger()
}
