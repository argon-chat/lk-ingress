// Copyright 2024 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package audiows

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/frostbyte73/core"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

// AudioWSDuplex handles a bidirectional audio WS session.
// One room connection is used to both publish the bot's audio track and
// subscribe to a target participant's audio track.
type AudioWSDuplex struct {
	sessionID string
	logger    logger.Logger
	room      *lksdk.Room
	conn      *websocket.Conn
	writeMu   sync.Mutex

	localTrack    *lksdk.LocalSampleTrack
	frameDuration time.Duration

	targetIdentity string
	targetSource   string

	fuse core.Fuse
}

func NewAudioWSDuplex(
	sessionID string,
	conn *websocket.Conn,
	room, identity, name string,
	targetIdentity, targetSource string,
	apiKey, apiSecret, wsUrl string,
	stereo bool,
	frameDurationMs int,
	trackName, trackSource, metadata string,
) (*AudioWSDuplex, error) {
	l := logger.GetLogger().WithValues(
		"sessionID", sessionID,
		"room", room,
		"identity", identity,
		"targetIdentity", targetIdentity,
	)

	d := &AudioWSDuplex{
		sessionID:      sessionID,
		logger:         l,
		conn:           conn,
		targetIdentity: targetIdentity,
		targetSource:   targetSource,
		frameDuration:  defaultFrameDuration,
	}

	if frameDurationMs > 0 {
		d.frameDuration = time.Duration(frameDurationMs) * time.Millisecond
	}

	// Build token — can publish + subscribe, not hidden (bot is a real participant)
	token, err := buildDuplexToken(apiKey, apiSecret, room, identity, name, metadata)
	if err != nil {
		return nil, err
	}

	// Room callbacks
	cb := lksdk.NewRoomCallback()
	cb.OnTrackSubscribed = d.onTrackSubscribed
	cb.OnTrackUnsubscribed = d.onTrackUnsubscribed
	cb.OnTrackPublished = func(pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
		d.trySubscribeTarget(pub, rp)
	}
	cb.OnParticipantConnected = func(rp *lksdk.RemoteParticipant) {
		if rp.Identity() == d.targetIdentity {
			for _, pub := range rp.TrackPublications() {
				if remotePub, ok := pub.(*lksdk.RemoteTrackPublication); ok {
					d.trySubscribeTarget(remotePub, rp)
				}
			}
		}
	}
	cb.OnParticipantDisconnected = func(rp *lksdk.RemoteParticipant) {
		if rp.Identity() == d.targetIdentity {
			l.Infow("target participant disconnected")
			d.sendJSON(subscribedMessage{
				Type:                "target_left",
				SessionID:           sessionID,
				ParticipantIdentity: targetIdentity,
			})
		}
	}
	cb.OnDisconnectedWithReason = func(reason lksdk.DisconnectionReason) {
		l.Infow("room disconnected", "reason", reason)
		d.fuse.Break()
		conn.Close()
	}

	// Join room — auto-subscribe disabled so we only subscribe to the target
	lkRoom := lksdk.NewRoom(cb)
	err = lkRoom.JoinWithToken(wsUrl, token, lksdk.WithAutoSubscribe(false))
	if err != nil {
		return nil, err
	}
	d.room = lkRoom

	// Publish local audio track
	if trackName == "" {
		trackName = "audio"
	}
	audioSource := livekit.TrackSource_MICROPHONE
	if trackSource == "screen_share_audio" {
		audioSource = livekit.TrackSource_SCREEN_SHARE_AUDIO
	}

	localTrack, err := lksdk.NewLocalSampleTrack(webrtc.RTPCodecCapability{MimeType: opusMimeType})
	if err != nil {
		lkRoom.Disconnect()
		return nil, err
	}
	d.localTrack = localTrack

	pubOpts := &lksdk.TrackPublicationOptions{
		Name:   trackName,
		Source: audioSource,
		Stereo: stereo,
	}
	_, err = lkRoom.LocalParticipant.PublishTrack(localTrack, pubOpts)
	if err != nil {
		lkRoom.Disconnect()
		return nil, err
	}

	// If target is already in the room, subscribe now
	if target := lkRoom.GetParticipantByIdentity(targetIdentity); target != nil {
		for _, pub := range target.TrackPublications() {
			if remotePub, ok := pub.(*lksdk.RemoteTrackPublication); ok {
				d.trySubscribeTarget(remotePub, target)
			}
		}
	}

	l.Infow("duplex session started", "stereo", stereo, "frameDurationMs", d.frameDuration.Milliseconds())

	return d, nil
}

func (d *AudioWSDuplex) trySubscribeTarget(pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) bool {
	if rp.Identity() != d.targetIdentity {
		return false
	}
	if !strings.HasPrefix(pub.MimeType(), "audio/") {
		return false
	}
	if d.targetSource != "" {
		switch d.targetSource {
		case "microphone":
			if pub.Source() != livekit.TrackSource_MICROPHONE {
				return false
			}
		case "screen_share_audio":
			if pub.Source() != livekit.TrackSource_SCREEN_SHARE_AUDIO {
				return false
			}
		}
	}
	d.logger.Infow("subscribing to target audio track", "trackSID", pub.SID())
	if err := pub.SetSubscribed(true); err != nil {
		d.logger.Warnw("failed to subscribe to track", err, "trackSID", pub.SID())
		return false
	}
	return true
}

func (d *AudioWSDuplex) onTrackSubscribed(track *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
	if rp.Identity() != d.targetIdentity {
		return
	}
	if track.Kind() != webrtc.RTPCodecTypeAudio {
		return
	}

	d.logger.Infow("subscribed to target audio track",
		"trackSID", pub.SID(),
		"codec", track.Codec().MimeType,
	)

	d.sendJSON(subscribedMessage{
		Type:                "subscribed",
		SessionID:           d.sessionID,
		TrackSID:            pub.SID(),
		ParticipantIdentity: rp.Identity(),
	})

	go d.readTrackLoop(track)
}

func (d *AudioWSDuplex) onTrackUnsubscribed(track *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
	if rp.Identity() == d.targetIdentity {
		d.logger.Infow("target audio track unsubscribed", "trackSID", pub.SID())
	}
}

// readTrackLoop reads audio from the subscribed remote track and writes to the WS.
func (d *AudioWSDuplex) readTrackLoop(track *webrtc.TrackRemote) {
	for {
		if d.fuse.IsBroken() {
			return
		}

		pkt, _, err := track.ReadRTP()
		if err != nil {
			if !d.fuse.IsBroken() {
				d.logger.Warnw("failed to read RTP from track", err)
			}
			return
		}

		if len(pkt.Payload) == 0 {
			continue
		}

		if err := d.writeOpusFrame(pkt.Payload); err != nil {
			if !d.fuse.IsBroken() {
				d.logger.Warnw("failed to write opus frame to WS", err)
			}
			return
		}
	}
}

// HandleOpusFrame validates and publishes an incoming Opus frame from the bot.
func (d *AudioWSDuplex) HandleOpusFrame(data []byte) error {
	if err := validateOpusFrame(data); err != nil {
		return err
	}
	return d.localTrack.WriteSample(media.Sample{
		Data:     data,
		Duration: d.frameDuration,
	}, nil)
}

func (d *AudioWSDuplex) writeOpusFrame(data []byte) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	d.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	return d.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (d *AudioWSDuplex) sendJSON(v interface{}) {
	data, _ := json.Marshal(v)
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	d.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	d.conn.WriteMessage(websocket.TextMessage, data)
}

func (d *AudioWSDuplex) runPingLoop() {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.writeMu.Lock()
			d.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			err := d.conn.WriteMessage(websocket.PingMessage, nil)
			d.writeMu.Unlock()
			if err != nil {
				return
			}
		case <-d.fuse.Watch():
			return
		}
	}
}

func (d *AudioWSDuplex) Close() {
	d.fuse.Once(func() {
		d.logger.Infow("closing duplex session")
		if d.room != nil {
			d.room.Disconnect()
		}
	})
}

func (d *AudioWSDuplex) Done() <-chan struct{} {
	return d.fuse.Watch()
}

func (d *AudioWSDuplex) SessionID() string {
	return d.sessionID
}

func buildDuplexToken(apiKey, apiSecret, room, identity, name, metadata string) (string, error) {
	canSubscribe := true
	canPublish := true
	at := auth.NewAccessToken(apiKey, apiSecret)
	grant := &auth.VideoGrant{
		RoomJoin:     true,
		Room:         room,
		CanSubscribe: &canSubscribe,
		CanPublish:   &canPublish,
	}
	at.SetIdentity(identity).
		SetName(name).
		SetVideoGrant(grant)
	if metadata != "" {
		at.SetMetadata(metadata)
	}
	return at.ToJWT()
}
