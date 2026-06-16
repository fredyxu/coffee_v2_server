package api

import (
	"encoding/json"
	"net/http"
	"time"

	"coffee_server/ws"
)

func RegisterRoutes(mux *http.ServeMux, hub *ws.Hub) {
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("GET /api/status", statusHandler(hub))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok": true,
	})
}

func statusHandler(hub *ws.Hub) http.HandlerFunc {
	startedAt := time.Now()

	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":              true,
			"clients":         hub.ClientCount(),
			"started_at":      startedAt.Format(time.RFC3339),
			"uptime_seconds":  int64(time.Since(startedAt).Seconds()),
			"server_time_utc": time.Now().UTC().Format(time.RFC3339),
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
