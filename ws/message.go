package ws

import "encoding/json"

type Message struct {
	Type     string          `json:"type"`
	DeviceID string          `json:"device_id,omitempty"`
	Room     string          `json:"room,omitempty"`
	Event    string          `json:"event,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
}
