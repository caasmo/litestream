package setup

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/benbjohnson/litestream/internal"
)

func InitLog(w io.Writer, level, typ string) {
	logOptions := slog.HandlerOptions{
		Level:       slog.LevelInfo,
		ReplaceAttr: internal.ReplaceAttr,
	}

	// Read log level from environment, if available.
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		level = v
	}

	switch strings.ToUpper(level) {
	case "TRACE":
		logOptions.Level = internal.LevelTrace
	case "DEBUG":
		logOptions.Level = slog.LevelDebug
	case "INFO":
		logOptions.Level = slog.LevelInfo
	case "WARN", "WARNING":
		logOptions.Level = slog.LevelWarn
	case "ERROR":
		logOptions.Level = slog.LevelError
	}

	var logHandler slog.Handler
	switch typ {
	case "json":
		logHandler = slog.NewJSONHandler(w, &logOptions)
	case "text", "":
		logHandler = slog.NewTextHandler(w, &logOptions)
	}

	// Set global default logger.
	slog.SetDefault(slog.New(logHandler))
}
