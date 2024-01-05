package logger

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

// Helper function to set the global minimum logging level and add a 'service' field to all log messages
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
