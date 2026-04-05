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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/livekit/ingress/pkg/config"
	"github.com/livekit/ingress/pkg/stats"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/logger"
	protoutils "github.com/livekit/protocol/utils"
)

const (
	wsReadLimit    = 64 * 1024 // 64KB max message size (Opus frames are typically < 4KB)
	wsPongWait     = 60 * time.Second
	wsPingInterval = 30 * time.Second
	wsWriteWait    = 10 * time.Second
)

// readyMessage is sent to the bot after the audio track is published.
type readyMessage struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
}

// errorMessage is sent to the bot on error before closing the connection.
type errorMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type AudioWSServer struct {
	conf      *config.Config
	apiKey    string
	apiSecret string
	wsUrl     string
	monitor   *stats.Monitor

	server   *http.Server
	upgrader websocket.Upgrader

	sessionsLock sync.Mutex
	sessions     map[string]*AudioWSSession
}

func NewAudioWSServer() *AudioWSServer {
	return &AudioWSServer{
		sessions: make(map[string]*AudioWSSession),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  wsReadLimit,
			WriteBufferSize: 4 * 1024,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
}

func (s *AudioWSServer) Start(conf *config.Config, monitor *stats.Monitor) error {
	s.conf = conf
	s.apiKey = conf.ApiKey
	s.apiSecret = conf.ApiSecret
	s.wsUrl = conf.WsUrl
	s.monitor = monitor

	mux := http.NewServeMux()
	mux.HandleFunc("/audio/connect", s.handleConnect)

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", conf.AudioWSPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Infow("starting AudioWS server", "port", conf.AudioWSPort)
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Errorw("AudioWS server failed", err)
		}
	}()

	return nil
}

func (s *AudioWSServer) Stop() {
	s.sessionsLock.Lock()
	sessions := make([]*AudioWSSession, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.sessionsLock.Unlock()

	for _, sess := range sessions {
		sess.Close()
	}

	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.server.Shutdown(ctx)
	}
}

func (s *AudioWSServer) handleConnect(w http.ResponseWriter, r *http.Request) {
	// Extract and validate JWT
	tokenStr := extractToken(r)
	if tokenStr == "" {
		http.Error(w, "missing authorization token", http.StatusUnauthorized)
		return
	}

	verifier, err := auth.ParseAPIToken(tokenStr)
	if err != nil {
		http.Error(w, "invalid token format", http.StatusUnauthorized)
		return
	}

	if verifier.APIKey() != s.apiKey {
		http.Error(w, "invalid API key", http.StatusForbidden)
		return
	}

	_, grants, err := verifier.Verify(s.apiSecret)
	if err != nil {
		http.Error(w, "token verification failed", http.StatusForbidden)
		return
	}

	if grants.Video == nil || grants.Video.Room == "" {
		http.Error(w, "token must include room grant", http.StatusForbidden)
		return
	}

	room := grants.Video.Room
	identity := grants.Identity
	name := grants.Name
	if identity == "" {
		http.Error(w, "token must include identity", http.StatusForbidden)
		return
	}

	// Check CPU capacity
	if !s.monitor.AcceptAudioWSSession() {
		http.Error(w, "server at capacity", http.StatusServiceUnavailable)
		return
	}

	// Parse audio config from query params
	stereo := r.URL.Query().Get("stereo") == "true" || r.URL.Query().Get("channels") == "2"
	frameDurationMs, _ := strconv.Atoi(r.URL.Query().Get("frame_duration_ms"))

	// Upgrade to WebSocket
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Errorw("websocket upgrade failed", err)
		return
	}

	sessionID := protoutils.NewGuid("AWS_")
	l := logger.GetLogger().WithValues("sessionID", sessionID, "room", room, "identity", identity)
	l.Infow("new audio WS connection", "stereo", stereo, "frameDurationMs", frameDurationMs)

	go s.runSession(conn, sessionID, room, identity, name, stereo, frameDurationMs, l)
}

func (s *AudioWSServer) runSession(conn *websocket.Conn, sessionID, room, identity, name string, stereo bool, frameDurationMs int, l logger.Logger) {
	defer conn.Close()

	conn.SetReadLimit(wsReadLimit)
	conn.SetReadDeadline(time.Now().Add(wsPongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(wsPongWait))
		return nil
	})

	// Create session (joins room, publishes track)
	session, err := NewAudioWSSession(
		context.Background(),
		sessionID,
		room, identity, name,
		s.apiKey, s.apiSecret, s.wsUrl,
		s.conf,
		stereo,
		frameDurationMs,
		func() {
			conn.Close()
		},
	)
	if err != nil {
		l.Errorw("failed to create audio WS session", err)
		writeError(conn, "failed to create session: "+err.Error())
		return
	}

	s.addSession(session)
	defer func() {
		s.removeSession(sessionID)
		session.Close()
		s.monitor.AudioWSSessionEnded()
	}()

	s.monitor.AudioWSSessionStarted()

	// Send ready message
	readyMsg, _ := json.Marshal(readyMessage{
		Type:      "ready",
		SessionID: sessionID,
	})
	conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	if err := conn.WriteMessage(websocket.TextMessage, readyMsg); err != nil {
		l.Warnw("failed to send ready message", err)
		return
	}

	// Start ping ticker
	pingTicker := time.NewTicker(wsPingInterval)
	defer pingTicker.Stop()

	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		for {
			select {
			case <-pingTicker.C:
				conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					return
				}
			case <-session.Done():
				return
			}
		}
	}()

	// Read loop — binary messages are Opus frames
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				l.Warnw("websocket read error", err)
			}
			return
		}

		if msgType != websocket.BinaryMessage {
			continue // Ignore non-binary messages after setup
		}

		if err := session.HandleOpusFrame(data); err != nil {
			l.Warnw("failed to handle opus frame", err)
			return
		}
	}
}

func (s *AudioWSServer) addSession(session *AudioWSSession) {
	s.sessionsLock.Lock()
	s.sessions[session.SessionID()] = session
	s.sessionsLock.Unlock()
}

func (s *AudioWSServer) removeSession(sessionID string) {
	s.sessionsLock.Lock()
	delete(s.sessions, sessionID)
	s.sessionsLock.Unlock()
}

func extractToken(r *http.Request) string {
	// Check Authorization header first
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" {
		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
			return parts[1]
		}
	}
	// Fall back to query parameter
	return r.URL.Query().Get("token")
}

func writeError(conn *websocket.Conn, msg string) {
	errMsg, _ := json.Marshal(errorMessage{
		Type:    "error",
		Message: msg,
	})
	conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	conn.WriteMessage(websocket.TextMessage, errMsg)
}
