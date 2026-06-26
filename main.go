package main

import (
	"bytes"
	_ "embed"
	"log"
	"net/http"

	"coffee_server/api"
	"coffee_server/config"
	"coffee_server/ws"
)

const (
	serverRuntimeVersion    = "coffee-server-batch-ulaw-v2-20260625-113718"
	serverRuntimeModifiedAt = "2026-06-25 11:37:18 CST"
)

// test
//
//go:embed tools/intercom-listener.html
var pttTestPage []byte

func main() {
	cfg := config.Load()

	hub := ws.NewHub()
	go hub.Run()

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, hub)
	ws.RegisterRoutes(mux, hub, cfg)
	mux.HandleFunc("GET /test/ptt", pttTestHandler)

	server := &http.Server{
		Addr:         cfg.Addr,
		Handler:      mux,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	log.Printf("coffee server listening on %s", cfg.Addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server failed: %v", err)
	}
}

func pttTestHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	page := bytes.ReplaceAll(pttTestPage, []byte("__SERVER_RUNTIME_VERSION__"), []byte(serverRuntimeVersion))
	page = bytes.ReplaceAll(page, []byte("__SERVER_RUNTIME_MODIFIED_AT__"), []byte(serverRuntimeModifiedAt))
	page = bytes.ReplaceAll(page, []byte("__AUDIO_PARSER_VERSION__"), []byte(ws.AudioParserVersion))
	_, _ = w.Write(page)
}
