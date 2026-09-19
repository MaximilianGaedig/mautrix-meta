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

package callbridge

import (
	"context"
	"errors"
	"io"
	"sync/atomic"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/rs/zerolog"
)

// RTPReader is the reading half of a *webrtc.TrackRemote.
type RTPReader interface {
	ReadRTP() (*rtp.Packet, interceptor.Attributes, error)
}

// RTPWriter is the writing half of a *webrtc.TrackLocalStaticRTP, which
// stamps each packet with the SSRC and payload type negotiated on its own
// PeerConnection.
type RTPWriter interface {
	WriteRTP(*rtp.Packet) error
}

// Rewriter keeps an outgoing stream continuous while the incoming one may
// change: it strips header extensions (their ids are negotiated per leg),
// and re-bases sequence numbers and timestamps when the source SSRC
// changes (e.g. after a peer renegotiates), so the receiver sees one
// monotonic stream. SSRC and payload type are replaced by the writer.
type Rewriter struct {
	started    bool
	inSSRC     uint32
	seqOffset  uint16
	tsOffset   uint32
	lastOutSeq uint16
	lastOutTS  uint32
}

// opusFrameTicks is one 20 ms Opus frame at 48 kHz, used to space the first
// packet of a new source after the last one of the old source.
const opusFrameTicks = 960

// Rewrite adjusts p in place.
func (r *Rewriter) Rewrite(p *rtp.Packet) {
	p.Header.Extension = false
	p.Header.Extensions = nil
	p.Header.ExtensionProfile = 0
	p.Header.Padding = false
	p.PaddingSize = 0
	if !r.started {
		r.started = true
		r.inSSRC = p.SSRC
	} else if p.SSRC != r.inSSRC {
		r.inSSRC = p.SSRC
		r.seqOffset = r.lastOutSeq + 1 - p.SequenceNumber
		r.tsOffset = r.lastOutTS + opusFrameTicks - p.Timestamp
	}
	p.SequenceNumber += r.seqOffset
	p.Timestamp += r.tsOffset
	r.lastOutSeq = p.SequenceNumber
	r.lastOutTS = p.Timestamp
}

// RelayStats counts relayed and dropped packets of one direction.
type RelayStats struct {
	Forwarded atomic.Uint64
	Dropped   atomic.Uint64
}

// Relay forwards Opus RTP from src to dst until ctx ends or src fails.
// Packets whose payload type is not srcOpusPT (RED, CN, DTMF) are dropped,
// since only Opus is negotiated on the other leg.
func Relay(ctx context.Context, src RTPReader, srcOpusPT uint8, dst RTPWriter, stats *RelayStats, log zerolog.Logger) error {
	var rw Rewriter
	loggedFirst := false
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		p, _, err := src.ReadRTP()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if p.PayloadType != srcOpusPT {
			stats.Dropped.Add(1)
			continue
		}
		rw.Rewrite(p)
		if err = dst.WriteRTP(p); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			return err
		}
		stats.Forwarded.Add(1)
		if !loggedFirst {
			loggedFirst = true
			log.Info().Msg("First audio packet relayed")
		}
	}
}
