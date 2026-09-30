package textfmt

import (
	"strings"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

func part(content *event.MessageEventContent) *bridgev2.ConvertedMessagePart {
	return &bridgev2.ConvertedMessagePart{Type: event.EventMessage, Content: content}
}

func TestMarkForwardedText(t *testing.T) {
	p := part(&event.MessageEventContent{MsgType: event.MsgText, Body: "see this"})
	MarkForwarded([]*bridgev2.ConvertedMessagePart{p})
	if p.Content.Body != "↷ Forwarded\n\nsee this" {
		t.Errorf("body = %q", p.Content.Body)
	}
	if !strings.Contains(p.Content.FormattedBody, "data-mx-forwarded-notice") || !strings.HasSuffix(p.Content.FormattedBody, "see this") {
		t.Errorf("formatted = %q", p.Content.FormattedBody)
	}
}

func TestMarkForwardedMediaWithoutCaptionKeepsTheFileName(t *testing.T) {
	p := part(&event.MessageEventContent{MsgType: event.MsgImage, Body: "image.jpg"})
	MarkForwarded([]*bridgev2.ConvertedMessagePart{p})
	if p.Content.FileName != "image.jpg" || p.Content.Body != "↷ Forwarded" {
		t.Errorf("content = %+v", p.Content)
	}
}

func TestMarkForwardedMediaWithCaption(t *testing.T) {
	p := part(&event.MessageEventContent{MsgType: event.MsgImage, Body: "my caption", FileName: "image.jpg"})
	MarkForwarded([]*bridgev2.ConvertedMessagePart{p})
	if p.Content.FileName != "image.jpg" || p.Content.Body != "↷ Forwarded\n\nmy caption" {
		t.Errorf("content = %+v", p.Content)
	}
}

func TestMarkForwardedSkipsHiddenPartsAndLabelsOnlyOne(t *testing.T) {
	hidden := part(&event.MessageEventContent{MsgType: event.MsgNotice, Body: "service"})
	hidden.DontBridge = true
	first := part(&event.MessageEventContent{MsgType: event.MsgText, Body: "one"})
	second := part(&event.MessageEventContent{MsgType: event.MsgText, Body: "two"})
	MarkForwarded([]*bridgev2.ConvertedMessagePart{hidden, first, second})
	if hidden.Content.Body != "service" || second.Content.Body != "two" || !strings.HasPrefix(first.Content.Body, "↷ Forwarded") {
		t.Errorf("hidden=%q first=%q second=%q", hidden.Content.Body, first.Content.Body, second.Content.Body)
	}
	MarkForwarded(nil)
}

func TestMarkForwardedLeavesLocationsAlone(t *testing.T) {
	p := part(&event.MessageEventContent{MsgType: event.MsgLocation, Body: "Cafe", GeoURI: "geo:1,2"})
	MarkForwarded([]*bridgev2.ConvertedMessagePart{p})
	if p.Content.Body != "Cafe" {
		t.Errorf("body = %q", p.Content.Body)
	}
}
