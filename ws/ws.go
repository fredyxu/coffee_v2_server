package ws

import (
	"log"
	"net/http"

	"coffee_server/config"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func RegisterRoutes(mux *http.ServeMux, hub *Hub, cfg config.Config) {
	mux.HandleFunc("GET /ws", handleWebSocket(hub, cfg))
}

func handleWebSocket(hub *Hub, cfg config.Config) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if cfg.Token != "" && r.URL.Query().Get("token") != cfg.Token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("websocket upgrade failed: %v", err)
			return
		}

		deviceID := r.URL.Query().Get("device_id")
		room := r.URL.Query().Get("room")
		if room == "" {
			room = "default"
		}

		client := newClient(hub, conn, deviceID, room)
		hub.register <- client

		log.Printf("websocket connected: device_id=%s room=%s remote=%s", deviceID, room, r.RemoteAddr)

		go client.writePump()
		client.readPump()
	}
}
