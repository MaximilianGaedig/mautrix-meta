package msgconv

import (
	"context"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/msgconv/mediadl"
)

// downloadWAMedia downloads and decrypts a WhatsApp media file with the client of the message being converted.
// It is a variable so that tests can convert messages without a network.
var downloadWAMedia = func(ctx context.Context, integral *waMediaTransport.WAMediaTransport_Integral, mediaType whatsmeow.MediaType) ([]byte, error) {
	client, ok := ctx.Value(mediadl.ContextKeyWAClient).(*whatsmeow.Client)
	if !ok || client == nil {
		return nil, fmt.Errorf("no WhatsApp client to download the media with")
	}
	return client.DownloadFB(ctx, integral, mediaType)
}

const vcardMimeType = "text/vcard"

// vcardFileName is a file name for the vCard of a contact: its display name when it has one.
func vcardFileName(displayName string) string {
	displayName = strings.NewReplacer("/", "_", "\\", "_").Replace(strings.TrimSpace(displayName))
	if displayName == "" {
		return "contact.vcf"
	}
	return displayName + ".vcf"
}

// joinVCards is the contacts' vCards as one file; nil when none has a vCard. A .vcf file may hold any number
// of cards.
func joinVCards(vcards []string) []byte {
	cards := make([]string, 0, len(vcards))
	for _, vcard := range vcards {
		if vcard = strings.TrimSpace(vcard); vcard != "" {
			cards = append(cards, vcard)
		}
	}
	if len(cards) == 0 {
		return nil
	}
	return []byte(strings.Join(cards, "\r\n") + "\r\n")
}

func noticePart(body string) *bridgev2.ConvertedMessagePart {
	return &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: &event.MessageEventContent{MsgType: event.MsgNotice, Body: body},
	}
}

// vcardPart uploads a vCard file and returns the file message for it. When the upload fails the fallback
// text is shown instead.
func (mc *MessageConverter) vcardPart(ctx context.Context, data []byte, fileName, fallback string) *bridgev2.ConvertedMessagePart {
	intent := ctx.Value(mediadl.ContextKeyIntent).(bridgev2.MatrixAPI)
	portal := ctx.Value(mediadl.ContextKeyPortal).(*bridgev2.Portal)
	mxc, file, err := intent.UploadMedia(ctx, portal.MXID, data, fileName, vcardMimeType)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to reupload contact card")
		return noticePart(fallback)
	}
	return &bridgev2.ConvertedMessagePart{
		Type: event.EventMessage,
		Content: &event.MessageEventContent{
			MsgType:  event.MsgFile,
			Body:     fileName,
			FileName: fileName,
			URL:      mxc,
			File:     file,
			Info:     &event.FileInfo{MimeType: vcardMimeType, Size: len(data)},
		},
		Extra: make(map[string]any),
	}
}

// waContactVCard is the vCard and display name of one shared contact. The card is inline in the message, or
// a file to download.
func (mc *MessageConverter) waContactVCard(ctx context.Context, contact *waConsumerApplication.ConsumerApplication_ContactMessage) (vcard, name string, err error) {
	transport, err := contact.Decode()
	if err != nil {
		return "", "", err
	}
	name = transport.GetAncillary().GetDisplayName()
	switch card := transport.GetIntegral().GetContact().(type) {
	case *waMediaTransport.ContactTransport_Integral_Vcard:
		return card.Vcard, name, nil
	case *waMediaTransport.ContactTransport_Integral_DownloadableVcard:
		data, err := downloadWAMedia(ctx, card.DownloadableVcard.GetIntegral(), whatsmeow.MediaDocument)
		if err != nil {
			return "", name, fmt.Errorf("%w: %w", bridgev2.ErrMediaDownloadFailed, err)
		}
		return string(data), name, nil
	}
	return "", name, nil
}

func (mc *MessageConverter) waContactToMatrix(ctx context.Context, contact *waConsumerApplication.ConsumerApplication_ContactMessage) *bridgev2.ConvertedMessagePart {
	vcard, name, err := mc.waContactVCard(ctx, contact)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to read shared contact")
		return noticePart("Failed to read shared contact")
	}
	data := joinVCards([]string{vcard})
	if data == nil {
		return noticePart("Shared a contact, but it came without details")
	}
	return mc.vcardPart(ctx, data, vcardFileName(name), "Shared contact: "+name)
}

// waContactsArrayToMatrix puts all the contacts in one vCard file, as a single contact is one.
func (mc *MessageConverter) waContactsArrayToMatrix(ctx context.Context, msg *waConsumerApplication.ConsumerApplication_ContactsArrayMessage) *bridgev2.ConvertedMessagePart {
	var cards, names []string
	for _, contact := range msg.GetContacts() {
		vcard, name, err := mc.waContactVCard(ctx, contact)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Msg("Failed to read shared contact")
			continue
		}
		if strings.TrimSpace(vcard) != "" {
			cards = append(cards, vcard)
			names = append(names, name)
		}
	}
	data := joinVCards(cards)
	if data == nil {
		return noticePart("Shared contacts, but none came with details")
	}
	fileName := fmt.Sprintf("%d contacts.vcf", len(cards))
	if name := msg.GetDisplayName(); name != "" {
		fileName = vcardFileName(name)
	}
	return mc.vcardPart(ctx, data, fileName, "Shared contacts: "+strings.Join(names, ", "))
}
