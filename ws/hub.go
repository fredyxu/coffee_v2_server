package ws

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	messageDateLayout = "2006-01-02"
	messageTimeLayout = "15:04:05"
	defaultRole       = "member"
	defaultLevel      = 0
)

var messageDatetimeLocation = time.FixedZone("Asia/Shanghai", 8*60*60)

type envelope struct {
	sender      *Client
	senderID    string
	messageType int
	room        string
	data        []byte
	date        string
	time        string
}

type Hub struct {
	register   chan *Client
	unregister chan *Client
	broadcast  chan envelope

	mu      sync.RWMutex
	clients map[*Client]struct{}
}

func NewHub() *Hub {
	return &Hub{
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan envelope, 256),
		clients:    make(map[*Client]struct{}),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = struct{}{}
			h.mu.Unlock()

		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			h.mu.Unlock()

		case msg := <-h.broadcast:
			var stale []*Client

			h.mu.RLock()
			for client := range h.clients {
				if client.room != msg.room {
					continue
				}

				data := msg.dataFor(client)
				select {
				case client.send <- outboundMessage{messageType: msg.messageType, data: data}:
				default:
					stale = append(stale, client)
				}
			}
			h.mu.RUnlock()

			if len(stale) > 0 {
				h.mu.Lock()
				for _, client := range stale {
					if _, ok := h.clients[client]; ok {
						delete(h.clients, client)
						close(client.send)
					}
				}
				h.mu.Unlock()
			}
		}
	}
}

func (msg envelope) dataFor(client *Client) []byte {
	if msg.messageType != websocket.TextMessage {
		return msg.data
	}

	var payload map[string]any
	if err := json.Unmarshal(msg.data, &payload); err != nil {
		return msg.data
	}

	if client == msg.sender {
		payload["from"] = "self"
	} else {
		payload["from"] = "other"
	}
	payload["sender_id"] = msg.senderID
	payload["date"] = msg.date
	payload["time"] = msg.time
	payload["role"] = defaultRole
	payload["level"] = defaultLevel

	data, err := json.Marshal(payload)
	if err != nil {
		return msg.data
	}
	return data
}

func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
