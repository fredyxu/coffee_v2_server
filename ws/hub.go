package ws

import "sync"

type envelope struct {
	sender      *Client
	messageType int
	room        string
	data        []byte
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

				select {
				case client.send <- outboundMessage{messageType: msg.messageType, data: msg.data}:
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

func (h *Hub) ClientCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.clients)
}
