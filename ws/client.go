package ws

import (
	"bytes"
	"encoding/json"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 180 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 64 * 1024
)

var newline = []byte{'\n'}
var space = []byte{' '}

type Client struct {
	hub      *Hub
	conn     *websocket.Conn
	send     chan outboundMessage
	deviceID string
	room     string
}

type outboundMessage struct {
	messageType int
	data        []byte
}

type keepaliveMessage struct {
	Type string `json:"type"`
}

type messageMetadata struct {
	Room string `json:"room"`
}

func newClient(hub *Hub, conn *websocket.Conn, deviceID string, room string) *Client {
	return &Client{
		hub:      hub,
		conn:     conn,
		send:     make(chan outboundMessage, 256),
		deviceID: deviceID,
		room:     room,
	}
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("websocket read error: device_id=%s room=%s err=%v", c.deviceID, c.room, err)
			}
			break
		}

		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}

		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		message = bytes.TrimSpace(bytes.ReplaceAll(message, newline, space))
		if messageType == websocket.TextMessage && c.handleKeepalive(message) {
			continue
		}
		room := c.room
		if messageType == websocket.TextMessage {
			room = c.messageRoom(message)
		}
		now := time.Now().In(messageDatetimeLocation)

		c.hub.broadcast <- envelope{
			sender:      c,
			senderID:    c.deviceID,
			messageType: messageType,
			room:        room,
			data:        message,
			date:        now.Format(messageDateLayout),
			time:        now.Format(messageTimeLayout),
		}
	}
}

func (c *Client) messageRoom(message []byte) string {
	var metadata messageMetadata
	if err := json.Unmarshal(message, &metadata); err != nil || metadata.Room == "" {
		return c.room
	}
	return metadata.Room
}

func (c *Client) handleKeepalive(message []byte) bool {
	var keepalive keepaliveMessage
	if err := json.Unmarshal(message, &keepalive); err != nil {
		return false
	}

	responseType := ""
	switch keepalive.Type {
	case "ping":
		responseType = "pong"
	case "heartbeat":
		responseType = "heartbeat_ack"
	default:
		return false
	}

	var ack map[string]any
	if err := json.Unmarshal(message, &ack); err != nil {
		ack = make(map[string]any)
	}
	ack["type"] = responseType
	ack["server_time_utc"] = time.Now().UTC().Format(time.RFC3339)

	payload, err := json.Marshal(ack)
	if err != nil {
		return true
	}

	select {
	case c.send <- outboundMessage{messageType: websocket.TextMessage, data: payload}:
	default:
		log.Printf("websocket keepalive response dropped: device_id=%s room=%s type=%s", c.deviceID, c.room, responseType)
	}

	return true
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			writer, err := c.conn.NextWriter(message.messageType)
			if err != nil {
				return
			}
			_, _ = writer.Write(message.data)

			queued := len(c.send)
			for i := 0; i < queued; i++ {
				next := <-c.send
				if next.messageType != message.messageType {
					c.send <- next
					break
				}
				if message.messageType == websocket.TextMessage {
					_, _ = writer.Write(newline)
				}
				_, _ = writer.Write(next.data)
			}

			if err := writer.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
