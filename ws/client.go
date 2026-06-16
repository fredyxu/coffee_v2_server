package ws

import (
	"bytes"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
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

		message = bytes.TrimSpace(bytes.ReplaceAll(message, newline, space))
		c.hub.broadcast <- envelope{
			sender:      c,
			messageType: messageType,
			room:        c.room,
			data:        message,
		}
	}
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
