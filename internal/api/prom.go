package api

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func promhttpHandler(s *Server) http.Handler {
	return promhttp.HandlerFor(s.reg, promhttp.HandlerOpts{})
}

func slogLevel(code int) slog.Level {
	switch {
	case code >= 500:
		return slog.LevelError
	case code >= 400:
		return slog.LevelInfo
	default:
		return slog.LevelDebug
	}
}
