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
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// audioTransport is the bot facing connection of a duplex session. Opus frames
// and JSON control messages travel over it, so the room plumbing in
// AudioWSDuplex stays transport agnostic and is shared by the WebSocket and
// WebTransport entry points.
//
// ReadFrame returns io.EOF once the client is gone for any expected reason, so
// callers only log errors that are actually unexpected.
type audioTransport interface {
	// ReadFrame blocks until the client sends the next Opus frame.
	ReadFrame() ([]byte, error)
	// WriteFrame sends one Opus frame to the client.
	WriteFrame(data []byte) error
	// SendJSON sends a control message to the client.
	SendJSON(v interface{}) error
	// Close tears down the underlying connection.
	Close() error
	// Kind names the transport in logs.
	Kind() string
}

// wsTransport carries a duplex session over a WebSocket: binary messages are
// Opus frames, text messages are JSON control messages, and liveness is kept by
// the WebSocket ping/pong exchange.
type wsTransport struct {
	conn *websocket.Conn

	writeMu sync.Mutex

	closeOnce sync.Once
	closed    chan struct{}
}

func newWSTransport(conn *websocket.Conn) *wsTransport {
	conn.SetReadLimit(wsReadLimit)
	conn.SetReadDeadline(time.Now().Add(wsPongWait))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(wsPongWait))
		return nil
	})

	t := &wsTransport{
		conn:   conn,
		closed: make(chan struct{}),
	}
	go t.keepalive()

	return t
}

func (t *wsTransport) ReadFrame() ([]byte, error) {
	for {
		msgType, data, err := t.conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) ||
				errors.Is(err, net.ErrClosed) {
				return nil, io.EOF
			}
			return nil, err
		}

		if msgType != websocket.BinaryMessage {
			continue // text messages from the bot are ignored after setup
		}
		return data, nil
	}
}

func (t *wsTransport) WriteFrame(data []byte) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	t.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	return t.conn.WriteMessage(websocket.BinaryMessage, data)
}

func (t *wsTransport) SendJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	t.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	return t.conn.WriteMessage(websocket.TextMessage, data)
}

func (t *wsTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)
	})
	return t.conn.Close()
}

func (t *wsTransport) Kind() string {
	return "websocket"
}

func (t *wsTransport) keepalive() {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			t.writeMu.Lock()
			t.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			err := t.conn.WriteMessage(websocket.PingMessage, nil)
			t.writeMu.Unlock()
			if err != nil {
				return
			}
		case <-t.closed:
			return
		}
	}
}
