package ws

import (
	"encoding/binary"
	"testing"

	"github.com/gorilla/websocket"
)

func TestParseAudioHeader(t *testing.T) {
	frame := make([]byte, audioHeaderSize+320)
	frame[0] = audioMagic0
	frame[1] = audioMagic1
	frame[2] = audioVersion
	frame[3] = audioCodecPCM16
	binary.LittleEndian.PutUint16(frame[4:6], 42)
	binary.LittleEndian.PutUint16(frame[6:8], 160)
	binary.LittleEndian.PutUint32(frame[8:12], 8000)

	header, ok, reason := parseAudioHeader(frame)
	if !ok {
		t.Fatalf("expected valid audio header, reason=%s", reason)
	}
	if header.Seq != 42 || header.Samples != 160 || header.SampleRate != 8000 {
		t.Fatalf("unexpected header: %+v", header)
	}
}

func TestParseAudioHeaderAcceptsFortyMillisecondFrame(t *testing.T) {
	frame := makeAudioFrameWithSamples(7, 320)

	header, ok, reason := parseAudioHeader(frame)
	if !ok {
		t.Fatalf("expected valid 40ms audio header, reason=%s", reason)
	}
	if header.Seq != 7 || header.Samples != 320 || header.SampleRate != 8000 {
		t.Fatalf("unexpected header: %+v", header)
	}
}

func TestParseAudioHeaderAcceptsG711ULawFrame(t *testing.T) {
	frame := makeAudioFrameWithCodecAndSamples(9, audioCodecG711ULaw, 160)

	header, ok, reason := parseAudioHeader(frame)
	if !ok {
		t.Fatalf("expected valid u-law audio header, reason=%s", reason)
	}
	if header.Seq != 9 || header.Codec != audioCodecG711ULaw || header.Samples != 160 || header.SampleRate != 8000 {
		t.Fatalf("unexpected header: %+v", header)
	}
}

func TestParseAudioHeaderRejectsInvalidFrame(t *testing.T) {
	tests := []struct {
		name  string
		frame []byte
	}{
		{name: "short", frame: []byte{audioMagic0}},
		{name: "bad magic", frame: makeAudioFrame('B', audioMagic1, audioVersion, audioCodecPCM16)},
		{name: "bad version", frame: makeAudioFrame(audioMagic0, audioMagic1, 2, audioCodecPCM16)},
		{name: "bad codec", frame: makeAudioFrame(audioMagic0, audioMagic1, audioVersion, 255)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok, _ := parseAudioHeader(tt.frame); ok {
				t.Fatal("expected invalid audio frame")
			}
		})
	}
}

func TestClientHandleAudioFramesSplitsBatch(t *testing.T) {
	hub := NewHub()
	client := testClient(hub, "speaker", "default")
	first := makeAudioFrameWithSeq(1)
	second := makeAudioFrameWithSeq(2)
	batch := append(append([]byte{}, first...), second...)

	if !client.handleAudioFrames(batch) {
		t.Fatal("expected binary batch to be handled")
	}

	assertAudioFrame(t, hub, 1, first, true, "")
	assertAudioFrame(t, hub, 2, second, true, "")
}

func TestClientHandleAudioFramesSplitsVariableLengthBatch(t *testing.T) {
	hub := NewHub()
	client := testClient(hub, "speaker", "default")
	first := makeAudioFrameWithSamples(1, 160)
	second := makeAudioFrameWithSamples(2, 320)
	batch := append(append([]byte{}, first...), second...)

	if !client.handleAudioFrames(batch) {
		t.Fatal("expected binary batch to be handled")
	}

	assertAudioFrame(t, hub, 1, first, true, "")
	assertAudioFrame(t, hub, 2, second, true, "")
}

func TestClientHandleAudioFramesSplitsG711ULawBatch(t *testing.T) {
	hub := NewHub()
	client := testClient(hub, "speaker", "default")
	first := makeAudioFrameWithCodecAndSamples(1, audioCodecG711ULaw, 160)
	second := makeAudioFrameWithCodecAndSamples(2, audioCodecG711ULaw, 160)
	batch := append(append([]byte{}, first...), second...)

	if !client.handleAudioFrames(batch) {
		t.Fatal("expected u-law binary batch to be handled")
	}

	assertAudioFrame(t, hub, 1, first, true, "")
	assertAudioFrame(t, hub, 2, second, true, "")
}

func TestClientHandleAudioFramesSplitsFiveFrameG711ULawBatch(t *testing.T) {
	hub := NewHub()
	client := testClient(hub, "speaker", "default")
	var batch []byte
	var frames [][]byte
	for i := uint16(0); i < 5; i++ {
		frame := makeAudioFrameWithCodecAndSamples(i, audioCodecG711ULaw, 160)
		frames = append(frames, frame)
		batch = append(batch, frame...)
	}

	if len(batch) != 860 {
		t.Fatalf("expected 5-frame u-law batch to be 860 bytes, got %d", len(batch))
	}
	if !client.handleAudioFrames(batch) {
		t.Fatal("expected 5-frame u-law binary batch to be handled")
	}

	for i, frame := range frames {
		assertAudioFrame(t, hub, uint16(i), frame, true, "")
	}
}

func TestClientHandleAudioFramesRejectsBadBatchLength(t *testing.T) {
	hub := NewHub()
	client := testClient(hub, "speaker", "default")
	frame := makeAudioFrameWithSeq(1)
	badBatch := append(append([]byte{}, frame...), 0)

	if !client.handleAudioFrames(badBatch) {
		t.Fatal("expected bad binary batch to be handled")
	}

	assertAudioFrame(t, hub, 1, frame, true, "")
	assertAudioFrame(t, hub, 0, []byte{0}, false, "bad_batch_len")
}

func TestHubAudioFrameForwardsOnlySpeakerToSameRoomOthers(t *testing.T) {
	hub := NewHub()
	speaker := testClient(hub, "speaker", "default")
	listener := testClient(hub, "listener", "default")
	otherRoom := testClient(hub, "other", "other")

	hub.clients[speaker] = struct{}{}
	hub.clients[listener] = struct{}{}
	hub.clients[otherRoom] = struct{}{}
	hub.speakers["default"] = roomSpeaker{client: speaker, callsign: "A"}

	frame := makeAudioFrame(audioMagic0, audioMagic1, audioVersion, audioCodecPCM16)
	hub.handleAudioFrame(testAudioFrame(speaker, "default", frame))

	assertNoMessage(t, speaker, "speaker")
	assertBinaryMessage(t, listener, frame, "listener")
	assertNoMessage(t, otherRoom, "other room")
}

func TestHubAudioFrameDropsNonSpeakerAndStoppedSpeaker(t *testing.T) {
	hub := NewHub()
	speaker := testClient(hub, "speaker", "default")
	intruder := testClient(hub, "intruder", "default")
	listener := testClient(hub, "listener", "default")

	hub.clients[speaker] = struct{}{}
	hub.clients[intruder] = struct{}{}
	hub.clients[listener] = struct{}{}
	hub.speakers["default"] = roomSpeaker{client: speaker, callsign: "A"}

	frame := makeAudioFrame(audioMagic0, audioMagic1, audioVersion, audioCodecPCM16)
	hub.handleAudioFrame(testAudioFrame(intruder, "default", frame))
	assertNoMessage(t, listener, "listener after intruder")

	delete(hub.speakers, "default")
	hub.handleAudioFrame(testAudioFrame(speaker, "default", frame))
	assertNoMessage(t, listener, "listener after stop")
}

func testAudioFrame(sender *Client, room string, data []byte) audioFrame {
	header, valid, reason := parseAudioHeader(data)
	return audioFrame{
		sender:     sender,
		room:       room,
		data:       data,
		valid:      valid,
		dropReason: reason,
		seq:        header.Seq,
		samples:    header.Samples,
		sampleRate: header.SampleRate,
		payloadLen: audioPayloadLen(data),
	}
}

func testClient(hub *Hub, deviceID string, room string) *Client {
	return &Client{
		hub:      hub,
		send:     make(chan outboundMessage, 4),
		deviceID: deviceID,
		room:     room,
	}
}

func makeAudioFrame(magic0 byte, magic1 byte, version byte, codec byte) []byte {
	frame := make([]byte, audioHeaderSize+320)
	frame[0] = magic0
	frame[1] = magic1
	frame[2] = version
	frame[3] = codec
	binary.LittleEndian.PutUint16(frame[4:6], 1)
	binary.LittleEndian.PutUint16(frame[6:8], 160)
	binary.LittleEndian.PutUint32(frame[8:12], 8000)
	return frame
}

func makeAudioFrameWithSeq(seq uint16) []byte {
	frame := makeAudioFrame(audioMagic0, audioMagic1, audioVersion, audioCodecPCM16)
	binary.LittleEndian.PutUint16(frame[4:6], seq)
	return frame
}

func makeAudioFrameWithSamples(seq uint16, samples uint16) []byte {
	return makeAudioFrameWithCodecAndSamples(seq, audioCodecPCM16, samples)
}

func makeAudioFrameWithCodecAndSamples(seq uint16, codec byte, samples uint16) []byte {
	payloadBytes := int(samples) * 2
	if codec == audioCodecG711ULaw {
		payloadBytes = int(samples)
	}
	frame := make([]byte, audioHeaderSize+payloadBytes)
	frame[0] = audioMagic0
	frame[1] = audioMagic1
	frame[2] = audioVersion
	frame[3] = codec
	binary.LittleEndian.PutUint16(frame[4:6], seq)
	binary.LittleEndian.PutUint16(frame[6:8], samples)
	binary.LittleEndian.PutUint32(frame[8:12], 8000)
	return frame
}

func assertAudioFrame(t *testing.T, hub *Hub, seq uint16, expected []byte, valid bool, reason string) {
	t.Helper()

	select {
	case frame := <-hub.audioFrame:
		if frame.seq != seq {
			t.Fatalf("got seq %d, expected %d", frame.seq, seq)
		}
		if frame.valid != valid {
			t.Fatalf("got valid %t, expected %t", frame.valid, valid)
		}
		if frame.dropReason != reason {
			t.Fatalf("got reason %q, expected %q", frame.dropReason, reason)
		}
		if string(frame.data) != string(expected) {
			t.Fatalf("got unexpected frame data")
		}
	default:
		t.Fatal("got no audio frame")
	}
}

func assertBinaryMessage(t *testing.T, client *Client, expected []byte, label string) {
	t.Helper()

	select {
	case msg := <-client.send:
		if msg.messageType != websocket.BinaryMessage {
			t.Fatalf("%s got message type %d", label, msg.messageType)
		}
		if string(msg.data) != string(expected) {
			t.Fatalf("%s got unexpected binary payload", label)
		}
	default:
		t.Fatalf("%s got no message", label)
	}
}

func assertNoMessage(t *testing.T, client *Client, label string) {
	t.Helper()

	select {
	case msg := <-client.send:
		t.Fatalf("%s got unexpected message type=%d bytes=%d", label, msg.messageType, len(msg.data))
	default:
	}
}
