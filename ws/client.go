package ws

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait               = 10 * time.Second
	pongWait                = 180 * time.Second
	pingPeriod              = 30 * time.Second
	maxMessageSize          = 64 * 1024
	defaultRoom             = "default"
	defaultCallsign         = "ZClubUser"
	audioHeaderSize         = 12
	audioMinSamples         = 160
	audioMaxSamples         = 320
	audioExpectedSampleRate = 8000
	audioMagic0             = 'C'
	audioMagic1             = 'A'
	audioVersion            = 1
	audioCodecPCM16         = 0
	audioCodecG711ULaw      = 1
)

const AudioParserVersion = "batch-ulaw-v2-20260625-113718"

var newline = []byte{'\n'}
var space = []byte{' '}

type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan outboundMessage
	deviceID  string
	room      string
	callsign  string
	fwVersion string
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

type intercomMessage struct {
	Type     string `json:"type"`
	Room     string `json:"room"`
	Callsign string `json:"callsign"`
}

type audioHeader struct {
	Magic0     byte
	Magic1     byte
	Version    byte
	Codec      byte
	Seq        uint16
	Samples    uint16
	SampleRate uint32
}

func newClient(hub *Hub, conn *websocket.Conn, deviceID string, room string, callsign string, fwVersion string) *Client {
	return &Client{
		hub:       hub,
		conn:      conn,
		send:      make(chan outboundMessage, 1024),
		deviceID:  deviceID,
		room:      normalizeRoom(room),
		callsign:  normalizeCallsign(callsign),
		fwVersion: fwVersion,
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
	c.conn.SetPingHandler(func(appData string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return c.conn.WriteControl(
			websocket.PongMessage,
			[]byte(appData),
			time.Now().Add(writeWait),
		)
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
		if messageType == websocket.BinaryMessage {
			if c.handleAudioFrames(message) {
				continue
			}
			continue
		}

		message = bytes.TrimSpace(bytes.ReplaceAll(message, newline, space))
		if c.handleKeepalive(message) {
			continue
		}
		if c.handleIntercom(message) {
			continue
		}

		room := c.room
		room = c.messageRoom(message)
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

func (c *Client) handleAudioFrames(message []byte) bool {
	if len(message) == 0 {
		return true
	}

	offset := 0
	for offset < len(message) {
		if len(message)-offset < audioHeaderSize {
			c.emitAudioFrame(message[offset:], false, "bad_batch_len")
			return true
		}
		header, valid, reason := parseAudioHeader(message[offset : offset+audioHeaderSize])
		frameSize := audioHeaderSize + audioPayloadBytesForHeader(header)
		if !valid || frameSize <= audioHeaderSize || offset+frameSize > len(message) {
			c.emitAudioFrame(message[offset:], false, reason)
			return true
		}
		c.emitAudioFrame(message[offset:offset+frameSize], true, "")
		offset += frameSize
	}
	return true
}

func (c *Client) emitAudioFrame(message []byte, parse bool, forcedReason string) {
	header, valid, reason := parseAudioHeader(message)
	if !parse {
		valid = false
		reason = forcedReason
	}
	frameData := append([]byte(nil), message...)

	c.hub.audioFrame <- audioFrame{
		sender:     c,
		room:       c.room,
		data:       frameData,
		valid:      valid,
		dropReason: reason,
		seq:        header.Seq,
		samples:    header.Samples,
		sampleRate: header.SampleRate,
		payloadLen: audioPayloadLen(frameData),
	}
}

func audioPayloadLen(message []byte) int {
	if len(message) <= audioHeaderSize {
		return 0
	}
	return len(message) - audioHeaderSize
}

func audioPayloadBytesForHeader(header audioHeader) int {
	switch header.Codec {
	case audioCodecG711ULaw:
		return int(header.Samples)
	default:
		return int(header.Samples) * 2
	}
}

func parseAudioHeader(message []byte) (audioHeader, bool, string) {
	if len(message) < audioHeaderSize {
		return audioHeader{}, false, "short_header"
	}

	header := audioHeader{
		Magic0:     message[0],
		Magic1:     message[1],
		Version:    message[2],
		Codec:      message[3],
		Seq:        binary.LittleEndian.Uint16(message[4:6]),
		Samples:    binary.LittleEndian.Uint16(message[6:8]),
		SampleRate: binary.LittleEndian.Uint32(message[8:12]),
	}
	if header.Magic0 != audioMagic0 || header.Magic1 != audioMagic1 {
		return header, false, "bad_magic"
	}
	if header.Version != audioVersion {
		return header, false, "bad_version"
	}
	if header.Codec != audioCodecPCM16 && header.Codec != audioCodecG711ULaw {
		return header, false, "bad_codec"
	}
	if header.Samples < audioMinSamples || header.Samples > audioMaxSamples {
		return header, false, "bad_samples"
	}
	if header.SampleRate != audioExpectedSampleRate {
		return header, false, "bad_sample_rate"
	}
	if payloadLen := audioPayloadLen(message); payloadLen > 0 && payloadLen != audioPayloadBytesForHeader(header) {
		return header, false, "bad_payload_len"
	}
	return header, true, ""
}

func (c *Client) messageRoom(message []byte) string {
	var metadata messageMetadata
	if err := json.Unmarshal(message, &metadata); err != nil || metadata.Room == "" {
		return c.room
	}
	return normalizeRoom(metadata.Room)
}

func (c *Client) handleIntercom(message []byte) bool {
	var req intercomMessage
	if err := json.Unmarshal(message, &req); err != nil {
		return false
	}

	room := normalizeRoomWithFallback(req.Room, c.room)
	callsign := normalizeCallsignWithFallback(req.Callsign, c.callsign)

	switch req.Type {
	case "intercom_talk_start":
		log.Printf("intercom start received: room=%s callsign=%s device_id=%s", room, callsign, c.deviceID)
		c.hub.intercomStart <- intercomStartRequest{
			client:   c,
			room:     room,
			callsign: callsign,
		}
		return true
	case "intercom_talk_stop":
		log.Printf("intercom stop received: room=%s callsign=%s device_id=%s", room, callsign, c.deviceID)
		c.hub.intercomStop <- intercomStopRequest{
			client:   c,
			room:     room,
			callsign: callsign,
		}
		return true
	default:
		return false
	}
}

func (c *Client) handleKeepalive(message []byte) bool {
	if strings.EqualFold(string(message), "PING") {
		c.sendKeepaliveResponse("pong", nil)
		return true
	}

	var keepalive keepaliveMessage
	if err := json.Unmarshal(message, &keepalive); err != nil {
		return false
	}

	responseType := ""
	switch strings.ToLower(keepalive.Type) {
	case "ping":
		responseType = "pong"
	case "heartbeat":
		responseType = "heartbeat_ack"
	default:
		return false
	}

	payload := make(map[string]any)
	if err := json.Unmarshal(message, &payload); err != nil {
		payload = nil
	}

	c.sendKeepaliveResponse(responseType, payload)
	return true
}

func (c *Client) sendKeepaliveResponse(responseType string, fields map[string]any) {
	if fields == nil {
		fields = make(map[string]any)
	}
	fields["type"] = responseType
	fields["server_time_utc"] = time.Now().UTC().Format(time.RFC3339)

	data, err := json.Marshal(fields)
	if err != nil {
		return
	}

	select {
	case c.send <- outboundMessage{messageType: websocket.TextMessage, data: data}:
	default:
		log.Printf("websocket keepalive response dropped: device_id=%s room=%s type=%s", c.deviceID, c.room, responseType)
	}
}

func normalizeRoom(room string) string {
	room = strings.TrimSpace(room)
	if room == "" {
		return defaultRoom
	}
	return room
}

func normalizeRoomWithFallback(room string, fallback string) string {
	room = strings.TrimSpace(room)
	if room != "" {
		return room
	}
	return normalizeRoom(fallback)
}

func normalizeCallsign(callsign string) string {
	callsign = strings.TrimSpace(callsign)
	if callsign == "" {
		return defaultCallsign
	}
	return callsign
}

func normalizeCallsignWithFallback(callsign string, fallback string) string {
	callsign = strings.TrimSpace(callsign)
	if callsign != "" {
		return callsign
	}
	return normalizeCallsign(fallback)
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

			if err := c.conn.WriteMessage(message.messageType, message.data); err != nil {
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
