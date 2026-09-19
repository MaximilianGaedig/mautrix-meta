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

package rtcsignal

import (
	"maps"
	"slices"
)

// ClientMediaUpdate is one entry of ClientMediaUpdateRequest.mediaUpdates: the
// client's tracks after the change (web client serializer "ClientMediaUpdate").
type ClientMediaUpdate struct {
	MediaStatus   map[string]bool      // 1 (track id -> enabled)
	MediaStatusEx map[string]TrackInfo // 2 (ClientMediaStatus)
}

// ClientMediaUpdateRequest (body member 15, CLIENT_MEDIA_UPDATE) is how a client
// changes its own media mid-call, e.g. turning the camera on: its tracks, and an
// offer to renegotiate with. From the web client's toThriftClientMediaUpdateRequest:
// fromVersion and toVersion both carry the client's new media state version.
type ClientMediaUpdateRequest struct {
	FromVersion  int64               // 1
	ToVersion    int64               // 2
	MediaUpdates []ClientMediaUpdate // 3
	Offer        *SessionDescription // 4
}

// ClientMediaUpdateResponse (body member 16) answers a ClientMediaUpdateRequest,
// with the peer's answer to its offer.
type ClientMediaUpdateResponse struct {
	CurrentVersion     int64                // 1
	Answer             *SessionDescription  // 2
	MediaStatus        map[string]TrackInfo // 3
	SDPOriginLocalID   string               // 4
	RenegotiationOffer *SessionDescription  // 5
	MediaPath          MediaPath            // 6
}

func (m *ClientMediaUpdateRequest) encode(w *writer) {
	w.fieldI64(1, m.FromVersion)
	w.fieldI64(2, m.ToVersion)
	w.fieldHeader(3, TypeList)
	w.listHeader(TypeStruct, len(m.MediaUpdates))
	for _, u := range m.MediaUpdates {
		w.structBegin()
		w.fieldHeader(1, TypeMap)
		w.mapHeader(TypeBinary, TypeTrue, len(u.MediaStatus))
		for _, k := range slices.Sorted(maps.Keys(u.MediaStatus)) {
			w.binaryValue([]byte(k))
			if u.MediaStatus[k] {
				w.b = append(w.b, byte(TypeTrue))
			} else {
				w.b = append(w.b, byte(TypeFalse))
			}
		}
		if u.MediaStatusEx != nil {
			w.mediaStatus(2, u.MediaStatusEx)
		}
		w.structEnd()
	}
	w.optSD(4, m.Offer)
}

func (m *ClientMediaUpdateRequest) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeI64:
			m.FromVersion, err = r.i64()
		case id == 2 && t == TypeI64:
			m.ToVersion, err = r.i64()
		case id == 3 && t == TypeList:
			err = r.structList(func() error {
				var u ClientMediaUpdate
				err := r.readStruct(func(id int16, t Type) (err error) {
					switch {
					case id == 1 && t == TypeMap:
						u.MediaStatus = map[string]bool{}
						err = r.stringMap(func(k string, _ Type) error {
							v, err := r.containerBool()
							u.MediaStatus[k] = v
							return err
						})
					case id == 2 && t == TypeStruct:
						u.MediaStatusEx, err = decodeMediaStatus(r)
					default:
						err = r.skip(t)
					}
					return
				})
				m.MediaUpdates = append(m.MediaUpdates, u)
				return err
			})
		case id == 4 && t == TypeStruct:
			m.Offer, err = decodeSD(r)
		default:
			err = r.skip(t)
		}
		return
	})
}

func (m *ClientMediaUpdateResponse) decode(r *reader) error {
	return r.readStruct(func(id int16, t Type) (err error) {
		switch {
		case id == 1 && t == TypeI64:
			m.CurrentVersion, err = r.i64()
		case id == 2 && t == TypeStruct:
			m.Answer, err = decodeSD(r)
		case id == 3 && t == TypeStruct:
			m.MediaStatus, err = decodeMediaStatus(r)
		case id == 4 && t == TypeBinary:
			m.SDPOriginLocalID, err = r.string()
		case id == 5 && t == TypeStruct:
			m.RenegotiationOffer, err = decodeSD(r)
		case id == 6 && t == TypeI32:
			var v int32
			v, err = r.i32()
			m.MediaPath = MediaPath(v)
		default:
			err = r.skip(t)
		}
		return
	})
}

func (m *ClientMediaUpdateResponse) encode(w *writer) {
	w.fieldI64(1, m.CurrentVersion)
	w.optSD(2, m.Answer)
	if m.MediaStatus != nil {
		w.mediaStatus(3, m.MediaStatus)
	}
	w.optString(4, m.SDPOriginLocalID)
	w.optSD(5, m.RenegotiationOffer)
	if m.MediaPath != 0 {
		w.fieldI32(6, int32(m.MediaPath))
	}
}

// NewClientMediaUpdate builds a CLIENT_MEDIA_UPDATE announcing the client's
// tracks at media state `version`, with an offer to renegotiate (e.g. adding video).
func (c *CallContext) NewClientMediaUpdate(version int64, tracks map[string]TrackInfo, offer string) *Message {
	status := make(map[string]bool, len(tracks))
	for id, ti := range tracks {
		status[id] = ti.Enabled
	}
	req := &ClientMediaUpdateRequest{
		FromVersion:  version,
		ToVersion:    version,
		MediaUpdates: []ClientMediaUpdate{{MediaStatus: status, MediaStatusEx: tracks}},
	}
	if offer != "" {
		req.Offer = &SessionDescription{SDP: offer}
	}
	return c.Request(TypeClientMediaUpdate, Body{ClientMediaUpdateRequest: req})
}
