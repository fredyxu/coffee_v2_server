package ws

import (
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	messageDateLayout = "2006-01-02"
	messageTimeLayout = "15:04:05"
	defaultRole       = "member"
	defaultLevel      = 0
	maxRooms          = 32
	maxRoomUsers      = 128

	speakerTimeoutCheckInterval = 5 * time.Second
	speakerAudioIdleTimeout     = 15 * time.Second
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

type roomSpeaker struct {
	client      *Client
	callsign    string
	startedAt   time.Time
	lastAudioAt time.Time
}

type intercomStartRequest struct {
	client   *Client
	room     string
	callsign string
}

type intercomStopRequest struct {
	client   *Client
	room     string
	callsign string
}

type audioFrame struct {
	sender     *Client
	room       string
	data       []byte
	valid      bool
	dropReason string
	seq        uint16
	samples    uint16
	sampleRate uint32
	payloadLen int
}

type roomAudioStats struct {
	segmentID          int64
	room               string
	startedAt          time.Time
	firstAudioAt       time.Time
	speakerDeviceID    string
	speakerCallsign    string
	inbound            int
	valid              int
	forwarded          int
	dropped            int
	lastDropReason     string
	lastEmittedDrop    string
	lastSeq            uint16
	hasSeq             bool
	seqGaps            int
	lastLen            int
	lastSamples        uint16
	lastSampleRate     uint32
	lastPayloadLen     int
	lastValid          bool
	lastSenderDeviceID string
	lastSenderCallsign string
}

type Hub struct {
	register      chan *Client
	unregister    chan *Client
	broadcast     chan envelope
	intercomStart chan intercomStartRequest
	intercomStop  chan intercomStopRequest
	audioFrame    chan audioFrame

	mu         sync.RWMutex
	clients    map[*Client]struct{}
	speakers   map[string]roomSpeaker
	audioStats map[string]*roomAudioStats
	segmentSeq int64
	revision   atomic.Uint64
}

func NewHub() *Hub {
	return &Hub{
		register:      make(chan *Client),
		unregister:    make(chan *Client),
		broadcast:     make(chan envelope, 256),
		intercomStart: make(chan intercomStartRequest),
		intercomStop:  make(chan intercomStopRequest),
		audioFrame:    make(chan audioFrame, 256),
		clients:       make(map[*Client]struct{}),
		speakers:      make(map[string]roomSpeaker),
		audioStats:    make(map[string]*roomAudioStats),
	}
}

func (h *Hub) Run() {
	speakerTimeoutTicker := time.NewTicker(speakerTimeoutCheckInterval)
	defer speakerTimeoutTicker.Stop()

	for {
		select {
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = struct{}{}
			h.mu.Unlock()
			h.broadcastRoomSnapshotsForChange(client.room)

		case client := <-h.unregister:
			if h.removeClient(client) {
				h.releaseSpeakersForClient(client)
				h.broadcastRoomSnapshotsForChange(client.room)
			}

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
				h.removeStaleClients(stale)
			}

		case req := <-h.intercomStart:
			h.handleIntercomStart(req)

		case req := <-h.intercomStop:
			h.handleIntercomStop(req)

		case frame := <-h.audioFrame:
			h.handleAudioFrame(frame)

		case <-speakerTimeoutTicker.C:
			h.releaseIdleSpeakers()
		}
	}
}

func (h *Hub) handleIntercomStart(req intercomStartRequest) {
	if !h.isClientActive(req.client) {
		return
	}

	h.mu.Lock()
	if speaker, ok := h.speakers[req.room]; ok {
		h.mu.Unlock()
		if speaker.client == req.client {
			h.sendIntercomStartAck(req.client, map[string]any{
				"type":     "intercom_talk_start_ack",
				"ok":       true,
				"room":     req.room,
				"callsign": req.callsign,
			})
			return
		}

		log.Printf("intercom start rejected: room=%s callsign=%s device_id=%s speaker=%s reason=busy", req.room, req.callsign, req.client.deviceID, speaker.callsign)
		h.sendIntercomStartAck(req.client, map[string]any{
			"type":    "intercom_talk_start_ack",
			"ok":      false,
			"reason":  "busy",
			"room":    req.room,
			"speaker": speaker.callsign,
		})
		return
	}

	now := time.Now()
	h.speakers[req.room] = roomSpeaker{
		client:      req.client,
		callsign:    req.callsign,
		startedAt:   now,
		lastAudioAt: now,
	}
	h.mu.Unlock()
	h.segmentSeq++
	h.audioStats[req.room] = &roomAudioStats{
		segmentID:       h.segmentSeq,
		room:            req.room,
		startedAt:       now,
		speakerDeviceID: req.client.deviceID,
		speakerCallsign: req.callsign,
		lastDropReason:  "none",
	}
	log.Printf("intercom start allowed: room=%s callsign=%s device_id=%s", req.room, req.callsign, req.client.deviceID)
	h.sendIntercomStartAck(req.client, map[string]any{
		"type":     "intercom_talk_start_ack",
		"ok":       true,
		"room":     req.room,
		"callsign": req.callsign,
	})
	h.broadcastIntercomTalking(req.room, req.callsign, true)
	h.emitAudioDiag(req.room, "segment_start", h.audioStats[req.room], req.client)
	h.broadcastRoomUsersSnapshot(req.room)
}

func (h *Hub) handleIntercomStop(req intercomStopRequest) {
	h.mu.Lock()
	speaker, ok := h.speakers[req.room]
	if !ok || speaker.client != req.client {
		h.mu.Unlock()
		return
	}

	delete(h.speakers, req.room)
	h.mu.Unlock()
	h.logAudioSegment(req.room, speaker, "stop")
	h.broadcastIntercomTalking(req.room, speaker.callsign, false)
	h.broadcastRoomUsersSnapshot(req.room)
}

func (h *Hub) handleAudioFrame(frame audioFrame) {
	stats := h.roomAudioStats(frame.room)
	stats.inbound++
	if stats.firstAudioAt.IsZero() {
		stats.firstAudioAt = time.Now()
	}
	stats.lastSenderDeviceID = frame.sender.deviceID
	stats.lastSenderCallsign = frame.sender.callsign
	stats.lastSeq = frame.seq
	stats.lastLen = len(frame.data)
	stats.lastSamples = frame.samples
	stats.lastSampleRate = frame.sampleRate
	stats.lastPayloadLen = frame.payloadLen
	stats.lastValid = frame.valid

	h.mu.RLock()
	speaker, ok := h.speakers[frame.room]
	fromSpeaker := ok && speaker.client == frame.sender
	h.mu.RUnlock()
	var speakerClient *Client
	if ok {
		speakerClient = speaker.client
	}

	if !frame.valid {
		h.dropAudioFrame(frame.room, stats, "invalid_frame", frame.dropReason, true, speakerClient)
		return
	}
	stats.valid++

	if !ok {
		h.dropAudioFrame(frame.room, stats, h.nonSpeakerDropReason(frame.sender), "no_active_speaker", true, speakerClient)
		return
	}
	if !fromSpeaker {
		h.dropAudioFrame(frame.room, stats, h.nonSpeakerDropReason(frame.sender), "sender_is_not_current_room_speaker", true, speakerClient)
		return
	}
	speaker.lastAudioAt = time.Now()
	h.mu.Lock()
	h.speakers[frame.room] = speaker
	h.mu.Unlock()

	var stale []*Client
	targets := 0
	writeFailed := false
	h.mu.RLock()
	for client := range h.clients {
		if client == frame.sender || client.room != frame.room {
			continue
		}

		select {
		case client.send <- outboundMessage{messageType: websocket.BinaryMessage, data: frame.data}:
			targets++
		default:
			writeFailed = true
			stale = append(stale, client)
		}
	}
	h.mu.RUnlock()

	h.recordSeq(frame, stats)
	if targets > 0 {
		stats.forwarded++
	}
	if writeFailed {
		h.dropAudioFrame(frame.room, stats, "ws_write_failed", "send_queue_full", true, speakerClient)
	}
	if targets == 0 {
		h.dropAudioFrame(frame.room, stats, "no_listener", "", true, speakerClient)
	}

	if len(stale) > 0 {
		h.removeStaleClients(stale)
	}
	if stats.inbound == 1 || stats.inbound%50 == 0 {
		h.emitAudioDiag(frame.room, "update", stats, frame.sender)
	}
}

func (h *Hub) roomAudioStats(room string) *roomAudioStats {
	stats, ok := h.audioStats[room]
	if !ok {
		stats = &roomAudioStats{
			room:           room,
			startedAt:      time.Now(),
			lastDropReason: "not_current_speaker",
		}
		h.audioStats[room] = stats
	}
	return stats
}

func (h *Hub) dropAudioFrame(room string, stats *roomAudioStats, reason string, detail string, emitOnChange bool, speaker *Client) {
	if reason == "" {
		reason = "unknown"
	}
	stats.dropped++
	stats.lastDropReason = reason
	if detail != "" {
		stats.lastDropReason = reason + ":" + detail
	}
	if emitOnChange && stats.lastEmittedDrop != stats.lastDropReason {
		stats.lastEmittedDrop = stats.lastDropReason
		h.emitAudioDiag(room, "audio_drop", stats, speaker)
	}
}

func (h *Hub) nonSpeakerDropReason(sender *Client) string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for room, speaker := range h.speakers {
		if speaker.client == sender && room != sender.room {
			return "wrong_room"
		}
	}
	return "not_current_speaker"
}

func (h *Hub) recordSeq(frame audioFrame, stats *roomAudioStats) {
	if !stats.hasSeq {
		stats.hasSeq = true
		stats.lastSeq = frame.seq
		return
	}
	expected := stats.lastSeq + 1
	if frame.seq != expected {
		stats.seqGaps++
	}
	stats.lastSeq = frame.seq
}

func (h *Hub) logAudioSegment(room string, speaker roomSpeaker, reason string) {
	stats, ok := h.audioStats[room]
	if !ok {
		stats = &roomAudioStats{
			startedAt:       time.Now(),
			speakerDeviceID: speaker.client.deviceID,
			speakerCallsign: speaker.callsign,
			lastDropReason:  "no_audio_stats",
		}
	}

	durationMs := time.Since(stats.startedAt).Milliseconds()
	log.Printf(
		"intercom audio segment: reason=%s segment_id=%d room=%s speaker_device_id=%s speaker_callsign=%s listener_count=%d inbound=%d valid=%d forwarded=%d dropped=%d last_drop_reason=%s first_audio_delay_ms=%v last_len=%d last_valid=%t last_seq=%d seq_gaps=%d last_samples=%d last_sample_rate=%d last_payload_len=%d last_sender_device_id=%s last_sender_callsign=%s duration_ms=%d",
		reason,
		stats.segmentID,
		room,
		stats.speakerDeviceID,
		stats.speakerCallsign,
		h.listenerCountForRoomExcluding(room, speaker.client),
		stats.inbound,
		stats.valid,
		stats.forwarded,
		stats.dropped,
		stats.lastDropReason,
		stats.firstAudioDelayMs(),
		stats.lastLen,
		stats.lastValid,
		stats.lastSeq,
		stats.seqGaps,
		stats.lastSamples,
		stats.lastSampleRate,
		stats.lastPayloadLen,
		stats.lastSenderDeviceID,
		stats.lastSenderCallsign,
		durationMs,
	)
	if stats.lastEmittedDrop != stats.lastDropReason && stats.lastDropReason != "" && stats.lastDropReason != "none" {
		stats.lastEmittedDrop = stats.lastDropReason
		h.emitAudioDiag(room, "audio_drop", stats, speaker.client)
	}
	h.emitAudioDiag(room, "summary", stats, speaker.client)
	delete(h.audioStats, room)
}

func (s *roomAudioStats) firstAudioDelayMs() any {
	if s == nil || s.firstAudioAt.IsZero() || s.startedAt.IsZero() {
		return nil
	}
	return s.firstAudioAt.Sub(s.startedAt).Milliseconds()
}

func (h *Hub) emitAudioDiag(room string, event string, stats *roomAudioStats, speaker *Client) {
	if stats == nil || stats.segmentID == 0 {
		return
	}
	durationMs := time.Since(stats.startedAt).Milliseconds()
	listenerCount := h.listenerCountForRoomExcluding(room, speaker)
	expectedFrames := durationMs / 20
	payload := map[string]any{
		"type":                  "intercom_audio_diag",
		"event":                 event,
		"segment_id":            stats.segmentID,
		"room":                  room,
		"speaker_callsign":      stats.speakerCallsign,
		"speaker_device_id":     stats.speakerDeviceID,
		"listener_count":        listenerCount,
		"inbound_binary_frames": stats.inbound,
		"valid_audio_frames":    stats.valid,
		"forwarded_frames":      stats.forwarded,
		"dropped_frames":        stats.dropped,
		"last_drop_reason":      stats.lastDropReason,
		"first_audio_delay_ms":  stats.firstAudioDelayMs(),
		"duration_ms":           durationMs,
		"expected_frames":       expectedFrames,
		"last_seq":              stats.lastSeq,
		"seq_gaps":              stats.seqGaps,
		"last_len":              stats.lastLen,
		"last_valid":            stats.lastValid,
		"last_samples":          stats.lastSamples,
		"last_sample_rate":      stats.lastSampleRate,
		"last_payload_len":      stats.lastPayloadLen,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	h.mu.RLock()
	for client := range h.clients {
		if client.room != room {
			continue
		}
		select {
		case client.send <- outboundMessage{messageType: websocket.TextMessage, data: data}:
		default:
		}
	}
	h.mu.RUnlock()
}

func (h *Hub) clientCountForRoom(room string) int {
	count := 0
	for client := range h.clients {
		if client.room == room {
			count++
		}
	}
	return count
}

func (h *Hub) nextRevision() uint64 {
	return h.revision.Add(1)
}

func (h *Hub) roomListSnapshot() map[string]any {
	rooms := []map[string]any{
		{
			"id":         defaultRoom,
			"name":       "大厅",
			"user_count": h.roomUserCount(defaultRoom),
			"locked":     false,
		},
	}
	truncated := false
	if len(rooms) > maxRooms {
		rooms = rooms[:maxRooms]
		truncated = true
	}
	return map[string]any{
		"type":        "room_list",
		"revision":    h.nextRevision(),
		"server_time": time.Now().In(messageDatetimeLocation).Format(time.RFC3339),
		"truncated":   truncated,
		"rooms":       rooms,
	}
}

func (h *Hub) roomUserCount(room string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.clientCountForRoom(room)
}

func (h *Hub) roomUsersSnapshot(room string) map[string]any {
	room = normalizeRoom(room)
	users := make([]map[string]any, 0)
	now := time.Now().In(messageDatetimeLocation).Format(time.RFC3339)

	h.mu.RLock()
	speaker, hasSpeaker := h.speakers[room]
	for client := range h.clients {
		if client.room != room {
			continue
		}
		users = append(users, map[string]any{
			"device_id":  client.deviceID,
			"callsign":   client.callsign,
			"fw_version": client.fwVersion,
			"talking":    hasSpeaker && speaker.client == client,
		})
		if len(users) >= maxRoomUsers {
			break
		}
	}
	h.mu.RUnlock()

	return map[string]any{
		"type":        "room_users",
		"room":        room,
		"revision":    h.nextRevision(),
		"server_time": now,
		"truncated":   h.roomUserCount(room) > maxRoomUsers,
		"users":       users,
	}
}

func (h *Hub) broadcastRoomSnapshotsForChange(room string) {
	h.broadcastRoomListSnapshot()
	h.broadcastRoomUsersSnapshot(room)
}

func (h *Hub) broadcastRoomListSnapshot() {
	h.broadcastJSONToAll(h.roomListSnapshot())
}

func (h *Hub) broadcastRoomUsersSnapshot(room string) {
	h.broadcastJSONToRoom(normalizeRoom(room), h.roomUsersSnapshot(room))
}

func (h *Hub) broadcastJSONToAll(payload map[string]any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	var stale []*Client
	h.mu.RLock()
	for client := range h.clients {
		select {
		case client.send <- outboundMessage{messageType: websocket.TextMessage, data: data}:
		default:
			stale = append(stale, client)
		}
	}
	h.mu.RUnlock()

	if len(stale) > 0 {
		h.removeStaleClients(stale)
	}
}

func (h *Hub) broadcastJSONToRoom(room string, payload map[string]any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	var stale []*Client
	h.mu.RLock()
	for client := range h.clients {
		if client.room != room {
			continue
		}
		select {
		case client.send <- outboundMessage{messageType: websocket.TextMessage, data: data}:
		default:
			stale = append(stale, client)
		}
	}
	h.mu.RUnlock()

	if len(stale) > 0 {
		h.removeStaleClients(stale)
	}
}

func (h *Hub) listenerCountForRoom(room string) int {
	h.mu.RLock()
	speaker, hasSpeaker := h.speakers[room]
	h.mu.RUnlock()
	var exclude *Client
	if hasSpeaker {
		exclude = speaker.client
	}
	return h.listenerCountForRoomExcluding(room, exclude)
}

func (h *Hub) listenerCountForRoomExcluding(room string, exclude *Client) int {
	count := 0
	h.mu.RLock()
	defer h.mu.RUnlock()

	for client := range h.clients {
		if client.room != room {
			continue
		}
		if exclude != nil && client == exclude {
			continue
		}
		count++
	}
	return count
}

func (h *Hub) releaseSpeakersForClient(client *Client) {
	type releasedSpeaker struct {
		room    string
		speaker roomSpeaker
	}
	var released []releasedSpeaker

	h.mu.Lock()
	for room, speaker := range h.speakers {
		if speaker.client != client {
			continue
		}
		released = append(released, releasedSpeaker{room: room, speaker: speaker})
		delete(h.speakers, room)
	}
	h.mu.Unlock()

	for _, item := range released {
		room := item.room
		speaker := item.speaker
		log.Printf("intercom speaker disconnected: room=%s callsign=%s device_id=%s", room, speaker.callsign, client.deviceID)
		h.logAudioSegment(room, speaker, "disconnect")
		h.broadcastIntercomTalking(room, speaker.callsign, false)
		h.broadcastRoomUsersSnapshot(room)
	}
}

func (h *Hub) releaseIdleSpeakers() {
	type releasedSpeaker struct {
		room    string
		reason  string
		speaker roomSpeaker
	}
	var released []releasedSpeaker
	now := time.Now()

	h.mu.Lock()
	for room, speaker := range h.speakers {
		if _, active := h.clients[speaker.client]; !active {
			released = append(released, releasedSpeaker{room: room, reason: "inactive", speaker: speaker})
			delete(h.speakers, room)
			continue
		}
		lastAudioAt := speaker.lastAudioAt
		if lastAudioAt.IsZero() {
			lastAudioAt = speaker.startedAt
		}
		if now.Sub(lastAudioAt) < speakerAudioIdleTimeout {
			continue
		}
		released = append(released, releasedSpeaker{room: room, reason: "audio_idle_timeout", speaker: speaker})
		delete(h.speakers, room)
	}
	h.mu.Unlock()

	for _, item := range released {
		switch item.reason {
		case "inactive":
			log.Printf("intercom speaker inactive: room=%s callsign=%s device_id=%s", item.room, item.speaker.callsign, item.speaker.client.deviceID)
		default:
			lastAudioAt := item.speaker.lastAudioAt
			if lastAudioAt.IsZero() {
				lastAudioAt = item.speaker.startedAt
			}
			log.Printf("intercom speaker timeout: room=%s callsign=%s device_id=%s idle=%s", item.room, item.speaker.callsign, item.speaker.client.deviceID, now.Sub(lastAudioAt).Round(time.Millisecond))
		}
		h.logAudioSegment(item.room, item.speaker, item.reason)
		h.broadcastIntercomTalking(item.room, item.speaker.callsign, false)
		h.broadcastRoomUsersSnapshot(item.room)
	}
}

func (h *Hub) broadcastIntercomTalking(room string, callsign string, talking bool) {
	payload := map[string]any{
		"type":     "intercom_talking",
		"room":     room,
		"callsign": callsign,
		"talking":  talking,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	log.Printf("intercom talking broadcast: room=%s callsign=%s talking=%t", room, callsign, talking)

	var stale []*Client
	h.mu.RLock()
	for client := range h.clients {
		if client.room != room {
			continue
		}

		select {
		case client.send <- outboundMessage{messageType: websocket.TextMessage, data: data}:
		default:
			stale = append(stale, client)
		}
	}
	h.mu.RUnlock()

	if len(stale) > 0 {
		h.removeStaleClients(stale)
	}
}

func (h *Hub) sendIntercomStartAck(client *Client, payload map[string]any) {
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	select {
	case client.send <- outboundMessage{messageType: websocket.TextMessage, data: data}:
	default:
		log.Printf("intercom start ack dropped: room=%v device_id=%s", payload["room"], client.deviceID)
	}
}

func (h *Hub) removeStaleClients(stale []*Client) {
	for _, client := range stale {
		if h.removeClient(client) {
			h.releaseSpeakersForClient(client)
		}
	}
}

func (h *Hub) removeClient(client *Client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, ok := h.clients[client]; !ok {
		return false
	}

	delete(h.clients, client)
	close(client.send)
	return true
}

func (h *Hub) isClientActive(client *Client) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	_, ok := h.clients[client]
	return ok
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
