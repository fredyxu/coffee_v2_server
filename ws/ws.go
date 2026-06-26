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
		callsign := r.URL.Query().Get("callsign")
		fwVersion := r.URL.Query().Get("fw_version")

		client := newClient(hub, conn, deviceID, room, callsign, fwVersion)
		hub.register <- client

		log.Printf("websocket connected: device_id=%s callsign=%s room=%s fw_version=%s remote=%s", client.deviceID, client.callsign, client.room, client.fwVersion, r.RemoteAddr)

		go client.writePump()
		client.readPump()
	}
}
