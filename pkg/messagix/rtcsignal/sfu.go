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

// MediaDescriptionUpdate is one m-section of a delta SERVER_MEDIA_UPDATE (MultiwayCommonSerializers).
type MediaDescriptionUpdate struct {
	Body string // 1: the whole m-section ("m=audio …" through its last a= line)
	MSID string // 2
	MID  string // 3
}

// SessionDescriptionUpdate (ServerMediaUpdateRequest field 16) is the delta form of a renegotiation the
// SFU sends to clients with SUPPORT_DELTA_SMU: m-sections keyed by m-line index, each replacing that
// section of the current remote description or appending a new one (a participant joined).
type SessionDescriptionUpdate struct {
	Media map[int32]MediaDescriptionUpdate // 1
}

// Indexes returns the m-line indexes of the update in order.
func (u *SessionDescriptionUpdate) Indexes() []int32 {
	return slices.Sorted(maps.Keys(u.Media))
}

func decodeSessionDescriptionUpdate(r *reader) (*SessionDescriptionUpdate, error) {
	u := &SessionDescriptionUpdate{Media: map[int32]MediaDescriptionUpdate{}}
	err := r.readStruct(func(id int16, t Type) error {
		if id != 1 || t != TypeMap {
			return r.skip(t)
		}
		kt, vt, n, err := r.mapHeader()
		if err != nil {
			return err
		}
		for range n {
			if kt != TypeI32 || vt != TypeStruct {
				if err = r.skipElem(kt); err == nil {
					err = r.skipElem(vt)
				}
				if err != nil {
					return err
				}
				continue
			}
			idx, err := r.i32()
			if err != nil {
				return err
			}
			var m MediaDescriptionUpdate
			err = r.readStruct(func(id int16, t Type) (err error) {
				switch {
				case id == 1 && t == TypeBinary:
					m.Body, err = r.string()
				case id == 2 && t == TypeBinary:
					m.MSID, err = r.string()
				case id == 3 && t == TypeBinary:
					m.MID, err = r.string()
				default:
					err = r.skip(t)
				}
				return
			})
			if err != nil {
				return err
			}
			u.Media[idx] = m
		}
		return nil
	})
	return u, err
}

func (w *writer) sessionDescriptionUpdate(id int16, u *SessionDescriptionUpdate) {
	w.fieldStruct(id, func() {
		w.fieldHeader(1, TypeMap)
		w.mapHeader(TypeI32, TypeStruct, len(u.Media))
		for _, idx := range u.Indexes() {
			m := u.Media[idx]
			w.zigzag(int64(idx))
			w.structBegin()
			w.optString(1, m.Body)
			w.optString(2, m.MSID)
			w.optString(3, m.MID)
			w.structEnd()
		}
	})
}

// GroupOfUsers (JoinResponse field 14, ConferenceStateRequest field 5): user ids that act as one
// participant (a user joined from several devices), shown under AliasID.
type GroupOfUsers struct {
	Users                    []string // 1 (set)
	AllowMultipleJoins       bool     // 2
	DismissOthersOnFirstJoin bool     // 3
	AliasID                  string   // 4
}

func decodeGroupsOfUsers(r *reader) ([]GroupOfUsers, error) {
	var out []GroupOfUsers
	err := r.structList(func() error {
		var g GroupOfUsers
		err := r.readStruct(func(id int16, t Type) (err error) {
			isBool := t == TypeTrue || t == TypeFalse
			switch {
			case id == 1 && (t == TypeSet || t == TypeList):
				g.Users, err = r.stringList()
			case id == 2 && isBool:
				g.AllowMultipleJoins = t == TypeTrue
			case id == 3 && isBool:
				g.DismissOthersOnFirstJoin = t == TypeTrue
			case id == 4 && t == TypeBinary:
				g.AliasID, err = r.string()
			default:
				err = r.skip(t)
			}
			return
		})
		out = append(out, g)
		return err
	})
	return out, err
}

func (w *writer) groupsOfUsers(id int16, groups []GroupOfUsers) {
	w.fieldHeader(id, TypeList)
	w.listHeader(TypeStruct, len(groups))
	for _, g := range groups {
		w.structBegin()
		w.fieldStringList(1, TypeSet, g.Users)
		w.fieldBool(2, g.AllowMultipleJoins)
		w.fieldBool(3, g.DismissOthersOnFirstJoin)
		w.optString(4, g.AliasID)
		w.structEnd()
	}
}
