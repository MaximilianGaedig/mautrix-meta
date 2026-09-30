package textfmt

import (
	"testing"

	"maunium.net/go/mautrix/event"
)

func TestParseGeoURI(t *testing.T) {
	lat, long, err := ParseGeoURI("geo:52.500000,13.400000;u=12")
	if err != nil || lat != 52.5 || long != 13.4 {
		t.Errorf("got %v %v %v", lat, long, err)
	}
	if _, _, err := ParseGeoURI("geo:1.5,2.5,300"); err != nil {
		t.Errorf("an altitude is allowed: %v", err)
	}
	for _, bad := range []string{"", "https://example.test", "geo:1", "geo:a,b", "geo:1,b"} {
		if _, _, err := ParseGeoURI(bad); err == nil {
			t.Errorf("%q must not parse", bad)
		}
	}
}

func TestLocationText(t *testing.T) {
	got, err := LocationText(&event.MessageEventContent{MsgType: event.MsgLocation, Body: "Cafe Example", GeoURI: "geo:52.5,13.4"})
	if err != nil || got != "Cafe Example\nhttps://www.google.com/maps?q=52.500000,13.400000" {
		t.Errorf("got %q %v", got, err)
	}
	got, _ = LocationText(&event.MessageEventContent{MsgType: event.MsgLocation, Body: "geo:52.5,13.4", GeoURI: "geo:52.5,13.4"})
	if got != "https://www.google.com/maps?q=52.500000,13.400000" {
		t.Errorf("a body that is just the geo URI adds nothing: %q", got)
	}
	if _, err := LocationText(&event.MessageEventContent{MsgType: event.MsgLocation, Body: "nowhere"}); err == nil {
		t.Error("a location without coordinates is an error")
	}
}
