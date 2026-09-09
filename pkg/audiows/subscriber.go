// Copyright 2024 LiveKit, Inc.
// Copyright 2026 Argon Inc. LLC
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

	lksdk "github.com/livekit/server-sdk-go/v2"

	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
)

type subscribedMessage struct {
	Status              string `json:"status"`
	SessionID           string `json:"session_id"`
	TrackSID            string `json:"track_sid,omitempty"`
	ParticipantIdentity string `json:"participant_identity,omitempty"`
}

type AudioWSSubscriber struct {
	sessionID string
	logger    logger.Logger
	room      *lksdk.Room
	conn      *websocket.Conn
	writeMu   sync.Mutex

	targetIdentity string
	targetSource   string // "microphone", "screen_share_audio", or "" for any

	fuse core.Fuse
}

func NewAudioWSSubscriber(
	sessionID string,
	conn *websocket.Conn,
	room, identity, name string,
	targetIdentity, targetSource string,
	apiKey, apiSecret, wsUrl string,
) (*AudioWSSubscriber, error) {
	l := logger.GetLogger().WithValues(
		"sessionID", sessionID,
		"room", room,
		"identity", identity,
		"targetIdentity", targetIdentity,
	)

	sub := &AudioWSSubscriber{
		sessionID:      sessionID,
		logger:         l,
		conn:           conn,
		targetIdentity: targetIdentity,
		targetSource:   targetSource,
	}

	// Build subscriber token (can subscribe, cannot publish, hidden)
	token, err := buildSubscriberToken(apiKey, apiSecret, room, identity, name)
	if err != nil {
		return nil, err
	}

	// Set up room callbacks
	cb := lksdk.NewRoomCallback()
	cb.OnTrackSubscribed = sub.onTrackSubscribed
	cb.OnTrackUnsubscribed = sub.onTrackUnsubscribed
	cb.OnTrackPublished = func(pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
		sub.trySubscribe(pub, rp)
	}
	cb.OnParticipantDisconnected = func(rp *lksdk.RemoteParticipant) {
		if rp.Identity() == sub.targetIdentity {
			l.Infow("target participant disconnected")
			sub.sendJSON(subscribedMessage{
				Status:              "target_left",
				SessionID:           sessionID,
				ParticipantIdentity: targetIdentity,
			})
			sub.conn.Close()
		}
	}

	// Join room with auto-subscribe disabled
	lkRoom := lksdk.NewRoom(cb)
	err = lkRoom.JoinWithToken(wsUrl, token, lksdk.WithAutoSubscribe(false))
	if err != nil {
		return nil, err
	}
	sub.room = lkRoom

	// Start ping ticker
	go sub.runPingLoop()

	// Check if target is already in the room
	found := false
	if target := lkRoom.GetParticipantByIdentity(targetIdentity); target != nil {
		for _, pub := range target.TrackPublications() {
			if remotePub, ok := pub.(*lksdk.RemoteTrackPublication); ok {
				if sub.trySubscribe(remotePub, target) {
					found = true
				}
			}
		}
	}

	if !found {
		l.Infow("target not yet in room, waiting")
		sub.sendJSON(subscribedMessage{
			Status:              "waiting",
			SessionID:           sessionID,
			ParticipantIdentity: targetIdentity,
		})
	}

	return sub, nil
}

func (s *AudioWSSubscriber) trySubscribe(pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) bool {
	if rp.Identity() != s.targetIdentity {
		return false
	}
	if !strings.HasPrefix(pub.MimeType(), "audio/") {
		return false
	}
	if s.targetSource != "" {
		switch s.targetSource {
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
	s.logger.Infow("subscribing to target audio track", "trackSID", pub.SID())
	if err := pub.SetSubscribed(true); err != nil {
		s.logger.Warnw("failed to subscribe to track", err, "trackSID", pub.SID())
		return false
	}
	return true
}

func (s *AudioWSSubscriber) onTrackSubscribed(track *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
	if rp.Identity() != s.targetIdentity {
		return
	}
	if track.Kind() != webrtc.RTPCodecTypeAudio {
		return
	}

	s.logger.Infow("subscribed to target audio track",
		"trackSID", pub.SID(),
		"codec", track.Codec().MimeType,
	)

	s.sendJSON(subscribedMessage{
		Status:              "subscribed",
		SessionID:           s.sessionID,
		TrackSID:            pub.SID(),
		ParticipantIdentity: rp.Identity(),
	})

	go s.readTrackLoop(track)
}

func (s *AudioWSSubscriber) onTrackUnsubscribed(track *webrtc.TrackRemote, pub *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
	if rp.Identity() == s.targetIdentity {
		s.logger.Infow("target audio track unsubscribed", "trackSID", pub.SID())
	}
}

func (s *AudioWSSubscriber) readTrackLoop(track *webrtc.TrackRemote) {
	for {
		if s.fuse.IsBroken() {
			return
		}

		pkt, _, err := track.ReadRTP()
		if err != nil {
			if !s.fuse.IsBroken() {
				s.logger.Warnw("failed to read RTP from track", err)
			}
			return
		}

		if len(pkt.Payload) == 0 {
			continue
		}

		if err := s.writeOpusFrame(pkt.Payload); err != nil {
			if !s.fuse.IsBroken() {
				s.logger.Warnw("failed to write opus frame to WS", err)
			}
			return
		}
	}
}

func (s *AudioWSSubscriber) writeOpusFrame(data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	return s.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (s *AudioWSSubscriber) sendJSON(v interface{}) {
	data, _ := json.Marshal(v)
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	s.conn.WriteMessage(websocket.TextMessage, data)
}

func (s *AudioWSSubscriber) runPingLoop() {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.writeMu.Lock()
			s.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			err := s.conn.WriteMessage(websocket.PingMessage, nil)
			s.writeMu.Unlock()
			if err != nil {
				return
			}
		case <-s.fuse.Watch():
			return
		}
	}
}

func (s *AudioWSSubscriber) Close() {
	s.fuse.Once(func() {
		s.logger.Infow("closing audio WS subscriber")
		if s.room != nil {
			s.room.Disconnect()
		}
	})
}

func (s *AudioWSSubscriber) Done() <-chan struct{} {
	return s.fuse.Watch()
}

func (s *AudioWSSubscriber) SessionID() string {
	return s.sessionID
}

func buildSubscriberToken(apiKey, apiSecret, room, identity, name string) (string, error) {
	canSubscribe := true
	canPublish := false
	at := auth.NewAccessToken(apiKey, apiSecret)
	at.SetIdentity(identity).
		SetName(name).
		SetVideoGrant(&auth.VideoGrant{
			RoomJoin:     true,
			Room:         room,
			CanSubscribe: &canSubscribe,
			CanPublish:   &canPublish,
			Hidden:       true,
		})
	return at.ToJWT()
}
