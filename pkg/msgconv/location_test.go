package msgconv

import (
	"context"
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/msgconv/textfmt"
)

func TestMatrixLocationIsSentAsTextWithMapLink(t *testing.T) {
	mc := &MessageConverter{HTMLParser: textfmt.NewMatrixParser(nil)}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: metaid.MakeFBPortalID(4242)}, Metadata: &metaid.PortalMetadata{}}}
	content := &event.MessageEventContent{
		MsgType: event.MsgLocation,
		Body:    "Cafe Example",
		GeoURI:  "geo:52.500000,13.400000",
	}
	tasks, err := mc.ToMeta(context.Background(), &messagix.Client{}, &event.Event{Type: event.EventMessage}, content, nil, nil, 77, false, portal)
	if err != nil {
		t.Fatalf("a location must be sent: %v", err)
	}
	task, ok := tasks[0].(*socket.SendMessageTask)
	if !ok {
		t.Fatalf("first task = %T", tasks[0])
	}
	if task.SendType != table.TEXT || !strings.Contains(task.Text, "Cafe Example") || !strings.Contains(task.Text, "52.500000,13.400000") {
		t.Errorf("task = %+v", task)
	}
}

func TestMatrixLocationWithoutCoordinatesIsRejected(t *testing.T) {
	mc := &MessageConverter{HTMLParser: textfmt.NewMatrixParser(nil)}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: metaid.MakeFBPortalID(4242)}, Metadata: &metaid.PortalMetadata{}}}
	_, err := mc.ToMeta(context.Background(), &messagix.Client{}, &event.Event{Type: event.EventMessage},
		&event.MessageEventContent{MsgType: event.MsgLocation, Body: "nowhere"}, nil, nil, 77, false, portal)
	if err == nil {
		t.Error("a location message without a geo URI cannot be sent")
	}
}
