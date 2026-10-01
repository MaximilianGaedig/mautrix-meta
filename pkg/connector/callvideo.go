// mautrix-meta - A Matrix-Facebook Messenger and Instagram DM puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package connector

import (
	"context"
	"slices"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2/callbridge"
)

// videoPayloadTyper is what the video relays ask of the leg a track came from (a *callbridge.Leg):
// every payload type its peer may send the track's codec under, the one the track began with first.
type videoPayloadTyper interface {
	VideoPayloadTypes(tr *webrtc.TrackRemote) []uint8
}

// relayLegVideo relays the video track tr of leg (read through src, which is tr itself outside
// tests) to dst under every payload type agreed for its codec. Messenger's phones agree H264 under
// two payload types and may move a track from one to the other mid-call (a screen shared in the
// camera's track); relaying only the payload type the track began with dropped everything after
// the move and froze the picture on its last frame.
func relayLegVideo(ctx context.Context, leg videoPayloadTyper, tr *webrtc.TrackRemote, src callbridge.RTPReader, dst callbridge.RTPWriter, stats *callbridge.RelayStats, log zerolog.Logger) error {
	return callbridge.RelayVideoCodec(ctx, src, leg.VideoPayloadTypes(tr), dst, stats, log,
		&callbridge.Rewriter{FrameTicks: callbridge.VideoFrameTicks})
}

// relayLegVideoTransformed is relayLegVideo for an encrypted call, where every frame goes through
// xf. The frame relay takes one payload type, so packets under the others agreed for the codec are
// read as that one: frames are taken apart and packetized again anyway, and dst stamps its own.
func relayLegVideoTransformed(ctx context.Context, leg videoPayloadTyper, tr *webrtc.TrackRemote, src callbridge.RTPReader, mime string, dst callbridge.RTPWriter, xf callbridge.FrameTransform, onLoss func(), stats *callbridge.FrameRelayStats, log zerolog.Logger) error {
	pts := leg.VideoPayloadTypes(tr)
	return callbridge.RelayVideoTransformed(ctx, &payloadTypeFolder{src: src, pts: pts}, pts[0], mime, dst, xf, onLoss, stats, log)
}

// payloadTypeFolder reads packets of any payload type in pts as packets of the first one. Other
// payload types (retransmissions) pass unchanged, for the reader to drop and count.
type payloadTypeFolder struct {
	src callbridge.RTPReader
	pts []uint8
}

func (f *payloadTypeFolder) ReadRTP() (*rtp.Packet, interceptor.Attributes, error) {
	p, attrs, err := f.src.ReadRTP()
	if err == nil && p != nil && slices.Contains(f.pts, p.PayloadType) {
		p.PayloadType = f.pts[0]
	}
	return p, attrs, err
}
