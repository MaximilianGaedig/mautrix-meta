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

// Package rtcsignal decodes and encodes the call signalling protocol that the
// facebook.com / messenger.com web client ("Zenon") speaks with Meta's
// multiway server (MWS).
//
// Transport: a DGW websocket at wss://gateway.facebook.com/ws/rpsignaling
// (same gateway framing as /ws/lightspeed, see package dgw) with query
// parameters x-dgw-app-stream-group=group1 and x-dgw-app-useUnifiedStream=true.
// One stream (id 0) is established with parameters "{}". Every signalling
// message is one DGW data frame with RequiresAck set; the peer acks it with an
// AckFrame.
//
// Payload: an Rtcdgwservice Request/Response envelope (Thrift compact) with
// serviceType MWS (1), whose payload is a Thrift compact RtcMessageHeader
// immediately followed by a Thrift compact RtcMessageBody union. There is no
// compression. The same header+body pair (prefixed by an empty
// MqttThriftHeader) is used on the legacy /t_rtc_multi MQTT topic.
//
// Every request is answered by a message with the same type and
// transactionId, responseStatusCode 200 and the matching *Response body (or
// an empty body for ICE_CANDIDATE and SUBSCRIPTION).
//
// SDP travels whole inside JoinRequest.offer (caller), RingRequest.offer
// (to callee), and ServerMediaUpdateRequest.answer (callee's answer relayed to
// the caller). ICE candidates trickle as ICE_CANDIDATE messages in both
// directions. E2EE key material: see e2ee.go.
//
// Schema names and field ids come from the web client's JS serializers
// (MultiwayCommonSerializers, E2eeStateSerializers, RtcdgwserviceSerializers,
// DtlsAuthenticationInfoSerializers).
package rtcsignal
