package main

import (
	"log"
	"net/http"

	"coffee_server/api"
	"coffee_server/config"
	"coffee_server/ws"
)

func main() {
	cfg := config.Load()

	hub := ws.NewHub()
	go hub.Run()

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, hub)
	ws.RegisterRoutes(mux, hub, cfg)

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
