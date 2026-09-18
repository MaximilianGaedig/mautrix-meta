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

// Package album marks Matrix media events that were sent together as one
// message (album / multi-attachment message) on the remote network, so that
// Matrix clients can group them.
//
// Each item is still bridged as its own event with a regular m.image/m.video/
// m.file/m.audio msgtype instead of a single com.beeper.gallery event. This is
// intentional: clients that don't understand albums keep rendering every item
// normally, while clients that do can group events with the same
// fi.mau.album.id.
package album

import (
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

// FieldKey is the top-level content field holding an [Info].
const FieldKey = "fi.mau.album"

// Info is the value of the fi.mau.album field.
type Info struct {
	// ID is a stable opaque identifier, unique within the portal.
	ID string `json:"id"`
	// Index is the 0-based position of the item within the album.
	Index int `json:"index"`
	// Count is the total number of items in the album, omitted when unknown.
	Count int `json:"count,omitempty"`
}

// IsMediaPart returns true if the part is a media message that can be part of an album.
func IsMediaPart(part *bridgev2.ConvertedMessagePart) bool {
	if part == nil || part.Type != event.EventMessage || part.Content == nil {
		return false
	}
	switch part.Content.MsgType {
	case event.MsgImage, event.MsgVideo, event.MsgFile, event.MsgAudio:
		return true
	default:
		return false
	}
}

// Set adds the album field to a single part.
func Set(part *bridgev2.ConvertedMessagePart, info Info) {
	if part.Extra == nil {
		part.Extra = make(map[string]any)
	}
	part.Extra[FieldKey] = &info
}

// Tag marks items as one album with the given ID. The position of a part in
// items is its index and len(items) is the count. Nil or non-media items
// (e.g. notices about failed downloads) keep their slot but aren't tagged.
// Nothing is tagged if there are fewer than 2 items.
func Tag(items []*bridgev2.ConvertedMessagePart, id string) {
	if len(items) < 2 {
		return
	}
	for i, part := range items {
		if IsMediaPart(part) {
			Set(part, Info{ID: id, Index: i, Count: len(items)})
		}
	}
}

// Get returns the album info in the given extra content map, if any.
func Get(extra map[string]any) *Info {
	info, _ := extra[FieldKey].(*Info)
	return info
}

// SetID replaces the album ID in the given extra content map, if there is an
// album field. Used for edits, where the converted content may be based on
// the edit event rather than the original message.
func SetID(extra map[string]any, id string) {
	if info := Get(extra); info != nil {
		cloned := *info
		cloned.ID = id
		extra[FieldKey] = &cloned
	}
}
