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
	"time"

	"github.com/frostbyte73/core"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/livekit/ingress/pkg/config"
	"github.com/livekit/ingress/pkg/lksdk_output"
	"github.com/livekit/ingress/pkg/params"
	"github.com/livekit/protocol/ingress"
	"github.com/livekit/protocol/livekit"
	"github.com/livekit/protocol/logger"
	protoutils "github.com/livekit/protocol/utils"
)

// noopStateNotifier satisfies the StateNotifier interface without doing anything.
// AudioWS sessions are not managed through the standard ingress state machinery.
type noopStateNotifier struct{}

func (n *noopStateNotifier) UpdateIngressState(_ context.Context, _ string, _ *livekit.IngressInfo) error {
	return nil
}

const (
	defaultFrameDuration = 20 * time.Millisecond
	opusMimeType         = "audio/opus"
)

type AudioWSSession struct {
	sessionID string
	logger    logger.Logger
	sdkOutput *lksdk_output.LKSDKOutput
	track     *lksdk_output.LocalTrack

	frameDuration time.Duration
	fuse          core.Fuse
}

func NewAudioWSSession(
	ctx context.Context,
	sessionID string,
	room, identity, name string,
	apiKey, apiSecret, wsUrl string,
	conf *config.Config,
	stereo bool,
	frameDurationMs int,
	onDisconnected func(),
) (*AudioWSSession, error) {
	l := logger.GetLogger().WithValues(
		"sessionID", sessionID,
		"room", room,
		"identity", identity,
	)

	token, err := ingress.BuildIngressToken(apiKey, apiSecret, room, identity, name, "", sessionID)
	if err != nil {
		return nil, err
	}

	resourceID := protoutils.NewGuid("AW_")
	enableTranscoding := false

	info := &livekit.IngressInfo{
		IngressId:           sessionID,
		StreamKey:           sessionID,
		RoomName:            room,
		ParticipantIdentity: identity,
		ParticipantName:     name,
		InputType:           livekit.IngressInput_URL_INPUT, // closest existing type; not dispatched through normal ingress flow
		EnableTranscoding:   &enableTranscoding,
		Audio: &livekit.IngressAudioOptions{
			Name:   "audio",
			Source: livekit.TrackSource_MICROPHONE,
		},
		Video: &livekit.IngressVideoOptions{},
		State: &livekit.IngressState{
			Status:     livekit.IngressState_ENDPOINT_PUBLISHING,
			ResourceId: resourceID,
			StartedAt:  time.Now().UnixNano(),
		},
	}

	p := &params.Params{
		IngressInfo: info,
		Config:      conf,
		WsUrl:       wsUrl,
		Token:       token,
	}
	p.SetLogger(l)
	p.SetStateNotifier(&noopStateNotifier{})

	sdkOutput, err := lksdk_output.NewLKSDKOutput(ctx, onDisconnected, p)
	if err != nil {
		return nil, err
	}

	track, err := sdkOutput.AddAudioTrack(opusMimeType, false, stereo)
	if err != nil {
		sdkOutput.Close()
		return nil, err
	}

	frameDuration := defaultFrameDuration
	if frameDurationMs > 0 {
		frameDuration = time.Duration(frameDurationMs) * time.Millisecond
	}

	s := &AudioWSSession{
		sessionID:     sessionID,
		logger:        l,
		sdkOutput:     sdkOutput,
		track:         track,
		frameDuration: frameDuration,
	}

	l.Infow("audio WS session started", "stereo", stereo, "frameDurationMs", frameDuration.Milliseconds())

	return s, nil
}

func (s *AudioWSSession) HandleOpusFrame(data []byte) error {
	return s.track.WriteSample(media.Sample{
		Data:     data,
		Duration: s.frameDuration,
	}, nil)
}

func (s *AudioWSSession) Close() {
	if s.fuse.IsBroken() {
		return
	}
	s.fuse.Break()

	s.logger.Infow("closing audio WS session")
	s.sdkOutput.Close()
}

func (s *AudioWSSession) Done() <-chan struct{} {
	return s.fuse.Watch()
}

func (s *AudioWSSession) SessionID() string {
	return s.sessionID
}
