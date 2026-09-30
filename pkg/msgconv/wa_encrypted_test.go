package msgconv

import (
	"context"
	"strings"
	"testing"

	"go.mau.fi/util/ptr"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/msgconv/mediadl"
)

type upload struct {
	data     []byte
	fileName string
	mime     string
}

type fakeIntent struct {
	bridgev2.MatrixAPI
	uploads []upload
}

func (f *fakeIntent) UploadMedia(ctx context.Context, roomID id.RoomID, data []byte, fileName, mimeType string) (id.ContentURIString, *event.EncryptedFileInfo, error) {
	f.uploads = append(f.uploads, upload{data, fileName, mimeType})
	return id.ContentURIString("mxc://example.test/file"), nil, nil
}

func waTestContext() (context.Context, *fakeIntent) {
	intent := &fakeIntent{}
	ctx := context.WithValue(context.Background(), mediadl.ContextKeyIntent, bridgev2.MatrixAPI(intent))
	ctx = context.WithValue(ctx, mediadl.ContextKeyPortal, &bridgev2.Portal{Portal: &database.Portal{}})
	return ctx, intent
}

func contactMessage(t *testing.T, name, vcard string) *waConsumerApplication.ConsumerApplication_ContactMessage {
	t.Helper()
	msg := &waConsumerApplication.ConsumerApplication_ContactMessage{}
	err := msg.Set(&waMediaTransport.ContactTransport{
		Integral:  &waMediaTransport.ContactTransport_Integral{Contact: &waMediaTransport.ContactTransport_Integral_Vcard{Vcard: vcard}},
		Ancillary: &waMediaTransport.ContactTransport_Ancillary{DisplayName: ptr.Ptr(name)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

const cardAda = "BEGIN:VCARD\nVERSION:3.0\nFN:Ada Example\nTEL:+15550100\nEND:VCARD"
const cardBob = "BEGIN:VCARD\nVERSION:3.0\nFN:Bob Example\nEND:VCARD\n"

func TestEncryptedContactBecomesVCardFile(t *testing.T) {
	ctx, intent := waTestContext()
	mc := &MessageConverter{}
	parts := mc.waConsumerToMatrix(ctx, &waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_ContactMessage{
			ContactMessage: contactMessage(t, "Ada Example", cardAda),
		},
	})
	if len(parts) != 1 {
		t.Fatalf("parts = %d", len(parts))
	}
	c := parts[0].Content
	if c.MsgType != event.MsgFile || c.FileName != "Ada Example.vcf" || c.Info.MimeType != "text/vcard" {
		t.Errorf("content = %+v info=%+v", c, c.Info)
	}
	if len(intent.uploads) != 1 || !strings.Contains(string(intent.uploads[0].data), "FN:Ada Example") {
		t.Fatalf("uploads = %+v", intent.uploads)
	}
}

func TestEncryptedContactsArrayIsOneFile(t *testing.T) {
	ctx, intent := waTestContext()
	mc := &MessageConverter{}
	parts := mc.waConsumerToMatrix(ctx, &waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_ContactsArrayMessage{
			ContactsArrayMessage: &waConsumerApplication.ConsumerApplication_ContactsArrayMessage{
				Contacts: []*waConsumerApplication.ConsumerApplication_ContactMessage{
					contactMessage(t, "Ada Example", cardAda),
					contactMessage(t, "Empty", ""),
					contactMessage(t, "Bob Example", cardBob),
				},
			},
		},
	})
	if len(parts) != 1 || parts[0].Content.MsgType != event.MsgFile {
		t.Fatalf("parts = %+v", parts)
	}
	if parts[0].Content.FileName != "2 contacts.vcf" {
		t.Errorf("file name = %q", parts[0].Content.FileName)
	}
	data := string(intent.uploads[0].data)
	if strings.Count(data, "BEGIN:VCARD") != 2 || !strings.Contains(data, "Bob Example") || strings.Contains(data, "Empty") {
		t.Errorf("file = %q", data)
	}
}

func TestEncryptedContactWithoutCardIsNotice(t *testing.T) {
	ctx, intent := waTestContext()
	mc := &MessageConverter{}
	part := mc.waContactToMatrix(ctx, contactMessage(t, "Nobody", ""))
	if part.Content.MsgType != event.MsgNotice || len(intent.uploads) != 0 {
		t.Errorf("part = %+v uploads = %d", part.Content, len(intent.uploads))
	}
}

func extendedText(t *testing.T) *waConsumerApplication.ConsumerApplication_ExtendedTextMessage {
	t.Helper()
	return &waConsumerApplication.ConsumerApplication_ExtendedTextMessage{
		Text:         &waCommon.MessageText{Text: ptr.Ptr("look https://example.test/a")},
		MatchedText:  ptr.Ptr("https://example.test/a"),
		CanonicalURL: ptr.Ptr("https://example.test/a?canonical=1"),
		Title:        ptr.Ptr("Example page"),
		Description:  ptr.Ptr("About the page"),
	}
}

func TestEncryptedLinkPreview(t *testing.T) {
	ctx, _ := waTestContext()
	mc := &MessageConverter{}
	parts := mc.waConsumerToMatrix(ctx, &waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_ExtendedTextMessage{ExtendedTextMessage: extendedText(t)},
	})
	previews := parts[0].Content.BeeperLinkPreviews
	if len(previews) != 1 {
		t.Fatalf("previews = %+v", previews)
	}
	p := previews[0]
	if p.MatchedURL != "https://example.test/a" || p.Title != "Example page" || p.Description != "About the page" || p.CanonicalURL != "https://example.test/a?canonical=1" {
		t.Errorf("preview = %+v", p)
	}
}

func TestEncryptedLinkPreviewWithoutURLIsExplicitlyEmpty(t *testing.T) {
	ctx, _ := waTestContext()
	msg := extendedText(t)
	msg.MatchedText = nil
	parts := (&MessageConverter{}).waConsumerToMatrix(ctx, &waConsumerApplication.ConsumerApplication_Content{
		Content: &waConsumerApplication.ConsumerApplication_Content_ExtendedTextMessage{ExtendedTextMessage: msg},
	})
	if got := parts[0].Content.BeeperLinkPreviews; got == nil || len(got) != 0 {
		t.Errorf("previews = %#v, want an empty non-nil list", got)
	}
}

func TestEncryptedLinkPreviewThumbnail(t *testing.T) {
	ctx, intent := waTestContext()
	old := downloadWAMedia
	downloadWAMedia = func(ctx context.Context, integral *waMediaTransport.WAMediaTransport_Integral, mediaType whatsmeow.MediaType) ([]byte, error) {
		if mediaType != whatsmeow.MediaImage || integral.GetDirectPath() != "/v/t/thumb" {
			t.Errorf("downloaded %q with %q", integral.GetDirectPath(), mediaType)
		}
		return []byte("\xff\xd8\xff\xe0not really a jpeg"), nil
	}
	defer func() { downloadWAMedia = old }()
	msg := extendedText(t)
	msg.PreviewType = waConsumerApplication.ConsumerApplication_ExtendedTextMessage_VIDEO.Enum()
	if err := msg.SetThumbnail(&waMediaTransport.ImageTransport{
		Integral: &waMediaTransport.ImageTransport_Integral{Transport: &waMediaTransport.WAMediaTransport{
			Integral: &waMediaTransport.WAMediaTransport_Integral{DirectPath: ptr.Ptr("/v/t/thumb")},
		}},
		Ancillary: &waMediaTransport.ImageTransport_Ancillary{Width: ptr.Ptr(uint32(320)), Height: ptr.Ptr(uint32(180))},
	}); err != nil {
		t.Fatal(err)
	}
	p := (&MessageConverter{}).waLinkPreviews(ctx, msg)[0]
	if p.ImageURL != "mxc://example.test/file" || p.ImageWidth != 320 || p.ImageHeight != 180 || p.ImageType != "image/jpeg" {
		t.Errorf("preview = %+v", p)
	}
	if p.Type != "video.other" || len(intent.uploads) != 1 {
		t.Errorf("type = %q uploads = %d", p.Type, len(intent.uploads))
	}
}
