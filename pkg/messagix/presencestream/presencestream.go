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

// Package presencestream implements Meta's "PresenceUnifiedJSON" request
// stream, which is how the Messenger and Instagram web clients learn whether
// their contacts are "Active now" or when they were last active.
//
// The web client (PresenceUnifiedClient in the static JS bundles) opens a DGW
// request stream with the method PresenceUnifiedJSON on the streamcontroller
// service (gated by the SC isolation rollout, which is enabled). The request
// body is a JSON object selecting the app family and polling mode, and the
// server answers with JSON publishes of the form
//
//	{"publishType": 1|2, "presenceUpdates": [{"userId": "...", "presenceStatus": 0|2, "lastActiveTimeSeconds": "..."}]}
//
// where publishType 1 is a full snapshot and 2 an incremental update, and
// presenceStatus 2 means active. Contacts outside the server-side buddy list
// are requested with an "additionalContacts" amendment, which the web client
// re-sends every 2.5 minutes while it is in the foreground.
package presencestream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/thrift"
	"go.mau.fi/mautrix-meta/pkg/instameow/thrift/requeststream"
)

const Method = "PresenceUnifiedJSON"

// PollInterval is how often the web client re-sends its additional contacts
// (PresenceUnifiedClient.POLLING_INTERVAL_MS).
const PollInterval = 150 * time.Second

// AppFamily is PresenceCommonPresenceCommonTypes.AppFamily.
type AppFamily int

const (
	AppFamilyFacebook  AppFamily = 1
	AppFamilyMessenger AppFamily = 2
	AppFamilyInstagram AppFamily = 3
)

// PollingMode is BladerunnerHandlersPresencePresenceHandlerTypes.PollingMode.
type PollingMode int

const (
	PollingModeBuddyList                 PollingMode = 1
	PollingModeAdditionalContacts        PollingMode = 2
	PollingModeDisabled                  PollingMode = 3
	PollingModeEnableRealtimeUpdatesOnly PollingMode = 4
)

// Availability is RealtimeNexusSessionDataTypes.PresenceAvailability, the
// state the client reports for its own user.
type Availability int

const (
	AvailabilityActive  Availability = 1
	AvailabilityIdle    Availability = 2
	AvailabilityOffline Availability = 3
)

// PublishEncoding is BladerunnerHandlersPresencePresenceHandlerTypes.PublishEncoding.
const PublishEncodingJSON = 2

// PublishType is BladerunnerHandlersPresencePresenceHandlerTypes.PublishType.
type PublishType int

const (
	PublishTypeFull        PublishType = 1
	PublishTypeIncremental PublishType = 2
)

// Status is PresenceCommonPresenceCommonTypes.PresenceStatus.
type Status int

const (
	StatusInactive Status = 0
	StatusActive   Status = 2
)

// PresenceCapabilityBit values (VoiceIpEnabled | WebrtcEnabled |
// ReceivePublishWithEIMUID), which the web clients combine and send as a
// decimal string.
const DefaultCapabilities = "268435466"

type ReportingRequest struct {
	Capabilities string       `json:"capabilities"`
	MutationID   string       `json:"mutationId"`
	Availability Availability `json:"availability"`
}

// Request is the request body of the stream.
type Request struct {
	AppFamily                AppFamily         `json:"appFamily"`
	PollingMode              PollingMode       `json:"pollingMode"`
	AppID                    string            `json:"appId"`
	PresenceReportingRequest *ReportingRequest `json:"presenceReportingRequest,omitempty"`
	PublishEncoding          int               `json:"publishEncoding"`
}

// StreamParameters are the DGW establish-stream headers, as produced by
// DGWRequestStreamUtils.convertHeaders with the x-dgw-app- prefix that
// DGWClient.constructConnectUrl adds to grouped stream headers.
type StreamParameters struct {
	Method      string `json:"x-dgw-app-XRSS-method"`
	Body        string `json:"x-dgw-app-xrs-body"`
	AcceptAck   string `json:"x-dgw-app-XRS-Accept-Ack"`
	HTTPReferer string `json:"x-dgw-app-XRSS-http_referer"`
}

func MakeStreamParameters(referer string) (json.RawMessage, error) {
	return json.Marshal(&StreamParameters{
		Method:      Method,
		Body:        "true",
		AcceptAck:   "RSAck",
		HTTPReferer: referer,
	})
}

// MakeInitPayload wraps the request body in a thrift RequestStream payload.
func MakeInitPayload(req *Request) ([]byte, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	return thrift.Marshal(&requeststream.Payload{
		RequestBody: &requeststream.RequestStreamBody{Body: body},
	})
}

type additionalContactsAmendment struct {
	Payload struct {
		AdditionalContacts struct {
			AdditionalContacts []string `json:"additionalContacts"`
		} `json:"additionalContacts"`
	} `json:"payload"`
}

// MakeAdditionalContactsAmendment asks the server to also publish the presence
// of the given user IDs (PresenceUnifiedClient.requestAdditonalContactsPresence).
func MakeAdditionalContactsAmendment(amendmentID int64, userIDs []string) ([]byte, error) {
	var amend additionalContactsAmendment
	amend.Payload.AdditionalContacts.AdditionalContacts = userIDs
	data, err := json.Marshal(&amend)
	if err != nil {
		return nil, err
	}
	return thrift.Marshal(&requeststream.Payload{
		Amend: &requeststream.AmendStream{
			AmendmentID: &amendmentID,
			Amendment:   data,
		},
	})
}

// MakeResponseAck acknowledges a response that asked for a device-level ack.
func MakeResponseAck(responseID int64) ([]byte, error) {
	return thrift.Marshal(&requeststream.Payload{
		Ack: &requeststream.StreamResponseAck{
			ResponseID: &responseID,
			Ack:        requeststream.Ack_Success,
		},
	})
}

// FlexInt64 accepts both JSON numbers and decimal strings. The web client
// parses both userId and lastActiveTimeSeconds with I64.of_string.
type FlexInt64 int64

func (f *FlexInt64) UnmarshalJSON(data []byte) error {
	data = bytes.Trim(data, `"`)
	if len(data) == 0 || string(data) == "null" {
		*f = 0
		return nil
	}
	val, err := strconv.ParseInt(string(data), 10, 64)
	if err != nil {
		floatVal, floatErr := strconv.ParseFloat(string(data), 64)
		if floatErr != nil {
			return fmt.Errorf("invalid int64 %q: %w", data, err)
		}
		val = int64(floatVal)
	}
	*f = FlexInt64(val)
	return nil
}

type Update struct {
	UserID                FlexInt64 `json:"userId"`
	PresenceStatus        Status    `json:"presenceStatus"`
	LastActiveTimeSeconds FlexInt64 `json:"lastActiveTimeSeconds"`
	Capabilities          FlexInt64 `json:"capabilities,omitempty"`
}

func (u *Update) IsActive() bool {
	return u.PresenceStatus == StatusActive
}

// LastActive returns the last active time, or the zero time if unknown.
func (u *Update) LastActive() time.Time {
	if u.LastActiveTimeSeconds <= 0 {
		return time.Time{}
	}
	return time.Unix(int64(u.LastActiveTimeSeconds), 0)
}

type Publish struct {
	PublishType     PublishType `json:"publishType"`
	PresenceUpdates []Update    `json:"presenceUpdates"`
}

func (p *Publish) IsFull() bool {
	return p.PublishType == PublishTypeFull
}

func ParsePublish(data []byte) (*Publish, error) {
	var pub Publish
	if err := json.Unmarshal(data, &pub); err != nil {
		return nil, err
	}
	return &pub, nil
}
