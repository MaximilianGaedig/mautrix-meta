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
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

func TestGroupRingClassification(t *testing.T) {
	cc := func(json string) []rtcsignal.DataMessage {
		return []rtcsignal.DataMessage{{Topic: collisionContextTopic, Data: []byte(json)}}
	}
	oneToOne := &rtcsignal.RingRequest{
		Caller:            "1",
		OtherParticipants: []string{"2"},
		Offer:             &rtcsignal.SessionDescription{SDP: "v=0"},
		AppMessages:       cc(`{"group_thread_id":null,"peer_id":"1"}`),
	}
	if isGroupRing(oneToOne) {
		t.Error("a 1:1 ring with an offer was taken for a group call")
	}
	group := &rtcsignal.RingRequest{Caller: "1", AppMessages: cc(`{"group_thread_id":"555","peer_id":null}`)}
	if !isGroupRing(group) || groupThreadOf(group.AppMessages) != "555" {
		t.Error("a ring naming a group thread wasn't taken for a group call")
	}
	if !isGroupRing(&rtcsignal.RingRequest{OtherParticipants: []string{"2", "3"}}) {
		t.Error("a ring with several other participants wasn't taken for a group call")
	}
}
