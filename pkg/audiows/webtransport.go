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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/livekit/ingress/pkg/config"
	"github.com/livekit/protocol/logger"
	protoutils "github.com/livekit/protocol/utils"
)

// Unidirectional streams open with a one byte header naming what they carry, so
// either side can tell a control stream from an audio stream.
const (
	wtStreamControl byte = 0x00 // newline delimited JSON control messages
	wtStreamAudio   byte = 0x01 // length prefixed Opus frames: [uint32 BE size][frame]
)

const (
	wtWriteWait    = 10 * time.Second
	wtIdleTimeout  = 60 * time.Second
	wtKeepAlive    = 20 * time.Second
	wtFrameQueue   = 128 // inbound frames buffered before new ones are dropped
	wtRecordHeader = 4
	wtCertValidity = 13 * 24 * time.Hour // browsers cap serverCertificateHashes certs at 14 days

	// How often the certificate files are restatted. Handshakes are rare here,
	// and a renewal only has to be noticed within minutes.
	wtCertCheckInterval = time.Minute

	// wtStreamRejected is the stream error code sent when a stream cannot be used.
	wtStreamRejected = 0x01
)

// startWebTransport serves the duplex audio protocol over WebTransport (HTTP/3
// over QUIC) alongside the WebSocket endpoints. Sessions are the same sessions:
// limits, CPU admission, session map and metrics are shared with the WS paths.
func (s *AudioWSServer) startWebTransport(conf *config.Config) error {
	tlsConf, err := wtTLSConfig(conf)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/audio/duplex", s.handleWTDuplex)

	s.wtServer = &webtransport.Server{
		H3: &http3.Server{
			Addr:      fmt.Sprintf(":%d", conf.AudioWTPort),
			TLSConfig: tlsConf,
			Handler:   mux,
			QUICConfig: &quic.Config{
				MaxIdleTimeout:  wtIdleTimeout,
				KeepAlivePeriod: wtKeepAlive,
				EnableDatagrams: true,
			},
		},
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	go func() {
		logger.Infow("starting AudioWT server", "port", conf.AudioWTPort)
		if err := s.wtServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Errorw("AudioWT server failed", err)
		}
	}()

	return nil
}

func (s *AudioWSServer) stopWebTransport() {
	if s.wtServer != nil {
		s.wtServer.Close()
	}
}

func (s *AudioWSServer) handleWTDuplex(w http.ResponseWriter, r *http.Request) {
	req, status, err := s.authorizeDuplex(r)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}

	if status, err := s.admitSession(); err != nil {
		http.Error(w, err.Error(), status)
		return
	}

	session, err := s.wtServer.Upgrade(w, r)
	if err != nil {
		logger.Warnw("webtransport upgrade failed", err)
		http.Error(w, "webtransport upgrade failed", http.StatusBadRequest)
		return
	}

	sessionID := protoutils.NewGuid("AWT_")
	l := logger.GetLogger().WithValues(
		"sessionID", sessionID,
		"room", req.room,
		"identity", req.identity,
		"targetIdentity", req.targetIdentity,
	)
	l.Infow("new audio WT duplex session",
		"stereo", req.stereo, "frameDurationMs", req.frameDurationMs,
		"trackName", req.trackName, "targetSource", req.targetSource,
	)

	go s.runDuplex(newWTTransport(session, l), sessionID, req, l)
}

// wtTransport carries a duplex session over a WebTransport session:
//
//   - Opus frames travel as QUIC datagrams in both directions. Datagrams are
//     unreliable, which is what realtime audio wants: a frame that arrives late
//     is worth less than the one that follows it.
//   - JSON control messages travel on a server opened unidirectional stream.
//   - Frames too large for a datagram, and every frame once a client turns out
//     not to support datagrams, fall back to a unidirectional audio stream.
//
// The bot may send its audio either way: as datagrams, or on a unidirectional
// stream it opens with the wtStreamAudio header.
type wtTransport struct {
	session *webtransport.Session
	logger  logger.Logger

	ctrlMu sync.Mutex
	ctrl   *webtransport.SendStream

	audioMu  sync.Mutex
	audioOut *webtransport.SendStream

	datagrams atomic.Bool
	dropped   atomic.Int64

	frames chan []byte

	closeOnce sync.Once
	closed    chan struct{}
}

func newWTTransport(session *webtransport.Session, l logger.Logger) *wtTransport {
	t := &wtTransport{
		session: session,
		logger:  l,
		frames:  make(chan []byte, wtFrameQueue),
		closed:  make(chan struct{}),
	}
	t.datagrams.Store(true)

	go t.readDatagrams()
	go t.acceptStreams()

	return t
}

func (t *wtTransport) Kind() string {
	return "webtransport"
}

func (t *wtTransport) ReadFrame() ([]byte, error) {
	select {
	case data := <-t.frames:
		return data, nil
	case <-t.session.Context().Done():
		return nil, io.EOF
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *wtTransport) WriteFrame(data []byte) error {
	if t.datagrams.Load() {
		err := t.session.SendDatagram(data)
		if err == nil {
			return nil
		}

		var tooLarge *quic.DatagramTooLargeError
		if !errors.As(err, &tooLarge) {
			// The peer never negotiated datagram support. Stop trying and send
			// the rest of the session over the reliable audio stream.
			t.datagrams.Store(false)
			t.logger.Infow("datagrams unavailable, falling back to the audio stream", "reason", err.Error())
		}
	}

	return t.writeAudioRecord(data)
}

func (t *wtTransport) SendJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	t.ctrlMu.Lock()
	defer t.ctrlMu.Unlock()

	// Opened on first use, which is the ready message: a session that never
	// reaches ready never costs a stream.
	if t.ctrl == nil {
		str, err := t.openStream(wtStreamControl)
		if err != nil {
			return err
		}
		t.ctrl = str
	}

	t.ctrl.SetWriteDeadline(time.Now().Add(wtWriteWait))
	_, err = t.ctrl.Write(data)
	return err
}

func (t *wtTransport) Close() error {
	t.closeOnce.Do(func() {
		close(t.closed)
		if n := t.dropped.Load(); n > 0 {
			t.logger.Infow("dropped inbound audio frames", "count", n)
		}
	})
	return t.session.CloseWithError(0, "")
}

// openStream opens a unidirectional stream and writes its type header.
func (t *wtTransport) openStream(streamType byte) (*webtransport.SendStream, error) {
	str, err := t.session.OpenUniStream()
	if err != nil {
		return nil, err
	}

	str.SetWriteDeadline(time.Now().Add(wtWriteWait))
	if _, err := str.Write([]byte{streamType}); err != nil {
		return nil, err
	}

	return str, nil
}

func (t *wtTransport) writeAudioRecord(data []byte) error {
	t.audioMu.Lock()
	defer t.audioMu.Unlock()

	if t.audioOut == nil {
		str, err := t.openStream(wtStreamAudio)
		if err != nil {
			return err
		}
		t.audioOut = str
	}

	// One write per record: a half written header desynchronizes the stream.
	buf := make([]byte, wtRecordHeader+len(data))
	binary.BigEndian.PutUint32(buf, uint32(len(data)))
	copy(buf[wtRecordHeader:], data)

	t.audioOut.SetWriteDeadline(time.Now().Add(wtWriteWait))
	_, err := t.audioOut.Write(buf)
	return err
}

func (t *wtTransport) readDatagrams() {
	ctx := t.session.Context()
	for {
		data, err := t.session.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		if len(data) == 0 {
			continue
		}
		if !t.pushFrame(data) {
			return
		}
	}
}

func (t *wtTransport) acceptStreams() {
	ctx := t.session.Context()
	for {
		str, err := t.session.AcceptUniStream(ctx)
		if err != nil {
			return
		}
		go t.readClientStream(str)
	}
}

func (t *wtTransport) readClientStream(str *webtransport.ReceiveStream) {
	var header [1]byte
	if _, err := io.ReadFull(str, header[:]); err != nil {
		return
	}

	if header[0] != wtStreamAudio {
		t.logger.Debugw("ignoring unidirectional stream of unknown type", "type", header[0])
		str.CancelRead(wtStreamRejected)
		return
	}

	if err := t.readAudioRecords(str); err != nil && !errors.Is(err, io.EOF) {
		t.logger.Debugw("audio stream ended", "error", err.Error())
	}
}

func (t *wtTransport) readAudioRecords(str *webtransport.ReceiveStream) error {
	var header [wtRecordHeader]byte
	for {
		if _, err := io.ReadFull(str, header[:]); err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return io.EOF
			}
			return err
		}

		size := binary.BigEndian.Uint32(header[:])
		if size == 0 || size > maxOpusFrameSize {
			str.CancelRead(wtStreamRejected)
			return fmt.Errorf("invalid audio record size %d", size)
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(str, data); err != nil {
			return err
		}
		if !t.pushFrame(data) {
			return nil
		}
	}
}

// pushFrame queues an inbound frame. The queue is dropped from rather than
// blocked on: stalling the reader would build latency the session never gets
// back, while a dropped Opus frame is a 20ms gap the decoder conceals.
func (t *wtTransport) pushFrame(data []byte) bool {
	select {
	case <-t.closed:
		return false
	default:
	}

	select {
	case t.frames <- data:
	default:
		t.dropped.Add(1)
	}
	return true
}

// wtTLSConfig loads the configured certificate, or falls back to a short lived
// self signed one. That fallback is an ECDSA P-256 certificate valid for under
// 14 days, which is what browsers require before they will accept it through
// WebTransport's serverCertificateHashes option; the fingerprint is logged so a
// client can pin it.
func wtTLSConfig(conf *config.Config) (*tls.Config, error) {
	if conf.AudioWTCertFile != "" && conf.AudioWTKeyFile != "" {
		// Read through a reloader rather than loading once: ACME renewals rewrite
		// these files under a long lived process, so a certificate loaded at
		// startup would go stale months before the process is next restarted.
		reloader, err := newCertReloader(conf.AudioWTCertFile, conf.AudioWTKeyFile)
		if err != nil {
			return nil, err
		}

		tlsConf := wtTLSConfigWithCert(tls.Certificate{})
		tlsConf.Certificates = nil
		tlsConf.GetCertificate = reloader.GetCertificate

		logger.Infow("AudioWT is using a certificate from disk",
			"certFile", conf.AudioWTCertFile,
			"notAfter", reloader.notAfter().Format(time.RFC3339),
		)

		return tlsConf, nil
	}

	cert, fingerprint, err := generateWTCert()
	if err != nil {
		return nil, err
	}

	logger.Infow("AudioWT is using a generated self signed certificate",
		"sha256", hex.EncodeToString(fingerprint),
		"sha256Base64", base64.StdEncoding.EncodeToString(fingerprint),
		"validUntil", time.Now().Add(wtCertValidity).Format(time.RFC3339),
	)

	return wtTLSConfigWithCert(cert), nil
}

// wtTLSConfigWithCert wraps a certificate for QUIC. The h3 ALPN is set here
// because the WebTransport server passes this config straight to the QUIC
// listener, without the http3 helper that would otherwise fill it in.
func wtTLSConfigWithCert(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{http3.NextProtoH3},
		MinVersion:   tls.VersionTLS13,
	}
}

// certReloader serves the certificate from disk and picks up renewals without a
// restart. Caddy and certbot rewrite the certificate and the key as two separate
// files, so a load can catch them mid renewal and fail; when that happens the
// certificate already loaded keeps being served and the next check tries again.
type certReloader struct {
	certFile string
	keyFile  string

	mu        sync.Mutex
	cert      *tls.Certificate
	stamps    [2]fileStamp
	checkedAt time.Time
}

// fileStamp is the cheap fingerprint used to notice a rewritten file.
type fileStamp struct {
	modTime time.Time
	size    int64
}

func newCertReloader(certFile, keyFile string) (*certReloader, error) {
	r := &certReloader{certFile: certFile, keyFile: keyFile}

	// Load eagerly: a wrong path or an unreadable key should fail startup rather
	// than the first bot that connects.
	if err := r.load(); err != nil {
		return nil, err
	}
	r.checkedAt = time.Now()

	return r, nil
}

func (r *certReloader) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if time.Since(r.checkedAt) < wtCertCheckInterval {
		return r.cert, nil
	}
	r.checkedAt = time.Now()

	if r.stat() == r.stamps {
		return r.cert, nil
	}

	if err := r.load(); err != nil {
		// A half written renewal, or the files briefly missing: keep serving what
		// is loaded and look again on the next check.
		logger.Warnw("AudioWT certificate reload failed, keeping the loaded one", err,
			"certFile", r.certFile)
		return r.cert, nil
	}

	logger.Infow("AudioWT certificate reloaded",
		"certFile", r.certFile,
		"notAfter", r.cert.Leaf.NotAfter.Format(time.RFC3339),
	)

	return r.cert, nil
}

func (r *certReloader) notAfter() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.cert.Leaf.NotAfter
}

// stat fingerprints both files. An unreadable file yields the zero stamp, which
// differs from any successful read and so schedules another attempt.
func (r *certReloader) stat() [2]fileStamp {
	var stamps [2]fileStamp

	for i, name := range []string{r.certFile, r.keyFile} {
		info, err := os.Stat(name)
		if err != nil {
			return [2]fileStamp{}
		}
		stamps[i] = fileStamp{modTime: info.ModTime(), size: info.Size()}
	}

	return stamps
}

// load reads the pair from disk. Stamps are taken before the read, so a file
// rewritten during it is picked up by the next check instead of being missed.
func (r *certReloader) load() error {
	stamps := r.stat()

	cert, err := tls.LoadX509KeyPair(r.certFile, r.keyFile)
	if err != nil {
		return err
	}

	// Parse the leaf once, so the expiry can be logged and handshakes are spared
	// the work.
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	cert.Leaf = leaf

	r.cert = &cert
	r.stamps = stamps

	return nil
}

func generateWTCert() (tls.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	now := time.Now()
	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "argon audio webtransport"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(wtCertValidity),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	sum := sha256.Sum256(der)

	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
	}, sum[:], nil
}
