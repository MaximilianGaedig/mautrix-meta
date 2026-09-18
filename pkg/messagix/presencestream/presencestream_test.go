package presencestream

import (
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/instameow/thrift"
	"go.mau.fi/mautrix-meta/pkg/instameow/thrift/requeststream"
)

func TestParsePublishStringAndNumberIDs(t *testing.T) {
	pub, err := ParsePublish([]byte(`{"publishType":1,"presenceUpdates":[
		{"userId":"100001234567890","presenceStatus":2,"lastActiveTimeSeconds":"1758290000","capabilities":"268435466"},
		{"userId":100009876543210,"presenceStatus":0,"lastActiveTimeSeconds":1758280000},
		{"userId":"42","presenceStatus":0}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	if !pub.IsFull() || len(pub.PresenceUpdates) != 3 {
		t.Fatalf("unexpected publish %+v", pub)
	}
	a, b, c := pub.PresenceUpdates[0], pub.PresenceUpdates[1], pub.PresenceUpdates[2]
	if a.UserID != 100001234567890 || !a.IsActive() || a.LastActive() != time.Unix(1758290000, 0) || a.Capabilities != 268435466 {
		t.Errorf("bad first update %+v", a)
	}
	if b.UserID != 100009876543210 || b.IsActive() || b.LastActive() != time.Unix(1758280000, 0) {
		t.Errorf("bad second update %+v", b)
	}
	if c.IsActive() || !c.LastActive().IsZero() {
		t.Errorf("bad third update %+v", c)
	}
}

func TestParsePublishIncremental(t *testing.T) {
	pub, err := ParsePublish([]byte(`{"publishType":2,"presenceUpdates":[]}`))
	if err != nil || pub.IsFull() {
		t.Fatalf("expected incremental publish, got %+v, %v", pub, err)
	}
}

func TestInitPayloadRoundTrip(t *testing.T) {
	data, err := MakeInitPayload(&Request{
		AppFamily:   AppFamilyFacebook,
		PollingMode: PollingModeBuddyList,
		AppID:       "2220391788200892",
		PresenceReportingRequest: &ReportingRequest{
			Capabilities: DefaultCapabilities,
			MutationID:   "x",
			Availability: AvailabilityIdle,
		},
		PublishEncoding: PublishEncodingJSON,
	})
	if err != nil {
		t.Fatal(err)
	}
	var payload requeststream.Payload
	if err = thrift.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err = json.Unmarshal(payload.RequestBody.Body, &body); err != nil {
		t.Fatal(err)
	}
	if body["appFamily"] != float64(1) || body["pollingMode"] != float64(1) || body["publishEncoding"] != float64(2) ||
		body["appId"] != "2220391788200892" {
		t.Errorf("unexpected body %v", body)
	}
	rep := body["presenceReportingRequest"].(map[string]any)
	if rep["availability"] != float64(2) || rep["capabilities"] != "268435466" {
		t.Errorf("unexpected reporting request %v", rep)
	}
}

func TestAmendmentRoundTrip(t *testing.T) {
	data, err := MakeAdditionalContactsAmendment(3, []string{"1", "2"})
	if err != nil {
		t.Fatal(err)
	}
	var payload requeststream.Payload
	if err = thrift.Unmarshal(data, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Amend == nil || payload.Amend.GetAmendmentID() != 3 {
		t.Fatalf("bad amend %+v", payload.Amend)
	}
	const expected = `{"payload":{"additionalContacts":{"additionalContacts":["1","2"]}}}`
	if string(payload.Amend.Amendment) != expected {
		t.Errorf("got %s, want %s", payload.Amend.Amendment, expected)
	}
}

func TestStreamParameters(t *testing.T) {
	params, err := MakeStreamParameters("https://www.facebook.com/messages/")
	if err != nil {
		t.Fatal(err)
	}
	const expected = `{"x-dgw-app-XRSS-method":"PresenceUnifiedJSON","x-dgw-app-xrs-body":"true","x-dgw-app-XRS-Accept-Ack":"RSAck","x-dgw-app-XRSS-http_referer":"https://www.facebook.com/messages/"}`
	if string(params) != expected {
		t.Errorf("got %s", params)
	}
}
