package api

import (
	"encoding/json"
	"net/http"
	"time"

	"coffee_server/ws"
)

type StatusMetadata struct {
	RuntimeVersion     string
	RuntimeModifiedAt  string
	AudioParserVersion string
}

func RegisterRoutes(mux *http.ServeMux, hub *ws.Hub, metadata StatusMetadata) {
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("GET /api/status", statusHandler(hub, metadata))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true,
	})
}

func statusHandler(hub *ws.Hub, metadata StatusMetadata) http.HandlerFunc {
	startedAt := time.Now()

	return func(w http.ResponseWriter, r *http.Request) {
		serverTime := time.Now()
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":                   true,
			"clients":              hub.ClientCount(),
			"started_at":           startedAt.Format(time.RFC3339),
			"uptime_seconds":       int64(time.Since(startedAt).Seconds()),
			"server_time":          serverTime.Format(time.RFC3339),
			"server_time_utc":      serverTime.UTC().Format(time.RFC3339),
			"runtime_version":      metadata.RuntimeVersion,
			"runtime_modified_at":  metadata.RuntimeModifiedAt,
			"audio_parser_version": metadata.AudioParserVersion,
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
