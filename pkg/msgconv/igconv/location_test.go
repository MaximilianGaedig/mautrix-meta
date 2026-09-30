package igconv

import (
	"context"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/msgconv/textfmt"
)

func TestMatrixLocationIsSentToInstagramAsTextWithMapLink(t *testing.T) {
	mc := &MessageConverter{HTMLParser: textfmt.NewMatrixParser(nil)}
	portal := &bridgev2.Portal{Portal: &database.Portal{
		PortalKey: networkid.PortalKey{ID: metaid.MakeFBPortalID(4242)},
		Metadata:  &metaid.PortalMetadata{IGID: "fake-thread"},
	}}
	req, err := mc.ToInstagram(context.Background(), nil, &event.Event{Type: event.EventMessage}, &event.MessageEventContent{
		MsgType: event.MsgLocation,
		Body:    "Cafe Example",
		GeoURI:  "geo:52.500000,13.400000",
	}, nil, 77, false, portal)
	if err != nil {
		t.Fatalf("a location must be sent: %v", err)
	}
	text, ok := req.(*slidetypes.SendTextRequest)
	if !ok {
		t.Fatalf("request = %T", req)
	}
	if !strings.Contains(text.Text.Value, "Cafe Example") || !strings.Contains(text.Text.Value, "52.500000,13.400000") {
		t.Errorf("text = %q", text.Text.Value)
	}
}
