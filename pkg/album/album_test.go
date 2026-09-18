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

package album

import (
	"encoding/json"
	"reflect"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

func mediaPart(msgType event.MessageType) *bridgev2.ConvertedMessagePart {
	return &bridgev2.ConvertedMessagePart{Type: event.EventMessage, Content: &event.MessageEventContent{MsgType: msgType}}
}

func expectInfo(t *testing.T, part *bridgev2.ConvertedMessagePart, expected *Info) {
	t.Helper()
	if got := Get(part.Extra); !reflect.DeepEqual(got, expected) {
		t.Errorf("expected album info %+v, got %+v", expected, got)
	}
}

func expectJSON(t *testing.T, value any, expected string) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var a, b any
	_ = json.Unmarshal(data, &a)
	_ = json.Unmarshal([]byte(expected), &b)
	if !reflect.DeepEqual(a, b) {
		t.Errorf("expected JSON %s, got %s", expected, data)
	}
}

func TestTag(t *testing.T) {
	img, vid, notice, file := mediaPart(event.MsgImage), mediaPart(event.MsgVideo), mediaPart(event.MsgNotice), mediaPart(event.MsgFile)
	Tag([]*bridgev2.ConvertedMessagePart{img, notice, vid, file}, "x:1")
	expectInfo(t, img, &Info{ID: "x:1", Index: 0, Count: 4})
	expectInfo(t, notice, nil)
	expectInfo(t, vid, &Info{ID: "x:1", Index: 2, Count: 4})
	expectInfo(t, file, &Info{ID: "x:1", Index: 3, Count: 4})
	expectJSON(t, img.Extra, `{"fi.mau.album":{"id":"x:1","index":0,"count":4}}`)
}

func TestTagSingleItem(t *testing.T) {
	img := mediaPart(event.MsgImage)
	Tag([]*bridgev2.ConvertedMessagePart{img}, "x:1")
	if img.Extra != nil {
		t.Errorf("single item must not be tagged, got %v", img.Extra)
	}
	Tag(nil, "x:1")
}

func TestTagSkipsStickersAndNil(t *testing.T) {
	sticker := &bridgev2.ConvertedMessagePart{Type: event.EventSticker, Content: &event.MessageEventContent{MsgType: event.MsgImage}}
	img := mediaPart(event.MsgImage)
	Tag([]*bridgev2.ConvertedMessagePart{sticker, nil, img}, "x:2")
	expectInfo(t, sticker, nil)
	expectInfo(t, img, &Info{ID: "x:2", Index: 2, Count: 3})
}

func TestCountOmittedWhenUnknown(t *testing.T) {
	img := mediaPart(event.MsgImage)
	Set(img, Info{ID: "x:3", Index: 1})
	expectJSON(t, img.Extra, `{"fi.mau.album":{"id":"x:3","index":1}}`)
}

func TestSetID(t *testing.T) {
	img, vid := mediaPart(event.MsgImage), mediaPart(event.MsgVideo)
	Tag([]*bridgev2.ConvertedMessagePart{img, vid}, "edit")
	SetID(vid.Extra, "orig")
	expectInfo(t, vid, &Info{ID: "orig", Index: 1, Count: 2})
	// The other part's info isn't shared and stays untouched.
	expectInfo(t, img, &Info{ID: "edit", Index: 0, Count: 2})
	SetID(map[string]any{}, "orig")
	SetID(nil, "orig")
}

func TestEditPartKeepsField(t *testing.T) {
	img, vid := mediaPart(event.MsgImage), mediaPart(event.MsgVideo)
	Tag([]*bridgev2.ConvertedMessagePart{img, vid}, "x:4")
	// ConvertedEditPart.Extra is put inside m.new_content by bridgev2.
	if got := Get(vid.ToEditPart(nil).Extra); !reflect.DeepEqual(got, Get(vid.Extra)) {
		t.Errorf("edit part lost album info: %+v", got)
	}
}
