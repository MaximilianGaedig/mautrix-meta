package msgconv

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"testing"

	"go.mau.fi/util/ptr"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/instamadilloAddMessage"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeActionLog"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeAdminMessage"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeCollection"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeLink"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeMedia"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeText"
	"go.mau.fi/whatsmeow/proto/instamadilloXmaContentRef"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/album"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var (
	testIGChat   = waTypes.NewJID("100000000000002", waTypes.MessengerServer)
	testIGSender = waTypes.NewJID("100000000000002", waTypes.MessengerServer)
)

func igPayload(content *instamadilloAddMessage.AddMessageContent) *instamadilloAddMessage.AddMessagePayload {
	return &instamadilloAddMessage.AddMessagePayload{Content: content}
}

func igTextContent(text string) *instamadilloAddMessage.AddMessageContent {
	return &instamadilloAddMessage.AddMessageContent{AddMessageContent: &instamadilloAddMessage.AddMessageContent_Text{
		Text: &instamadilloCoreTypeText.Text{Text: ptr.Ptr(text)},
	}}
}

func igMediaContent(media *instamadilloCoreTypeMedia.Media) *instamadilloAddMessage.AddMessageContent {
	return &instamadilloAddMessage.AddMessageContent{AddMessageContent: &instamadilloAddMessage.AddMessageContent_Media{Media: media}}
}

var (
	testKey  = bytes.Repeat([]byte{7}, 32)
	testHash = bytes.Repeat([]byte{9}, 32)
)

func igRef(mime string) *instamadilloCoreTypeMedia.CommonMediaTransport {
	return &instamadilloCoreTypeMedia.CommonMediaTransport{
		MediaKey:      ptr.Ptr(base64.StdEncoding.EncodeToString(testKey)),
		FileSHA256:    ptr.Ptr(hex.EncodeToString(testHash)),
		FileEncSHA256: ptr.Ptr(string(testHash)),
		DirectPath:    ptr.Ptr("/m1/v/t/fake-path"),
		Mimetype:      ptr.Ptr(mime),
		FileLength:    ptr.Ptr(int32(12)),
	}
}

type downloaded struct {
	integral  *waMediaTransport.WAMediaTransport_Integral
	mediaType whatsmeow.MediaType
}

func fakeDownloads(t *testing.T) *[]downloaded {
	t.Helper()
	var calls []downloaded
	old := downloadWAMedia
	downloadWAMedia = func(ctx context.Context, integral *waMediaTransport.WAMediaTransport_Integral, mediaType whatsmeow.MediaType) ([]byte, error) {
		calls = append(calls, downloaded{integral, mediaType})
		return []byte("fake file"), nil
	}
	t.Cleanup(func() { downloadWAMedia = old })
	return &calls
}

// convertIG runs a whole encrypted Instagram message through the converter, the way the connector does.
func convertIG(t *testing.T, mc *MessageConverter, payload *instamadilloAddMessage.AddMessagePayload) (*bridgev2.ConvertedMessage, *fakeIntent) {
	t.Helper()
	intent := &fakeIntent{}
	portal := &bridgev2.Portal{Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: metaid.MakeWAPortalID(testIGChat)}}}
	evt := &events.FBMessage{Message: payload}
	evt.Info.Chat = testIGChat
	evt.Info.Sender = testIGSender
	evt.Info.ID = "FAKEMSGID1"
	return mc.WhatsAppToMatrix(context.Background(), portal, nil, nil, nil, intent, "wa:fake", evt), intent
}

func TestInstagramEncryptedText(t *testing.T) {
	cm, _ := convertIG(t, &MessageConverter{}, igPayload(igTextContent("hello from instagram")))
	if len(cm.Parts) != 1 {
		t.Fatalf("parts = %d", len(cm.Parts))
	}
	c := cm.Parts[0].Content
	if c.MsgType != event.MsgText || c.Body != "hello from instagram" {
		t.Errorf("content = %+v", c)
	}
	if strings.Contains(c.Body, "Unsupported") {
		t.Error("text must not be reported as unsupported")
	}
}

func TestInstagramEncryptedLike(t *testing.T) {
	cm, _ := convertIG(t, &MessageConverter{}, igPayload(&instamadilloAddMessage.AddMessageContent{
		AddMessageContent: &instamadilloAddMessage.AddMessageContent_Like{Like: &instamadilloAddMessage.Like{}},
	}))
	if cm.Parts[0].Content.Body != "❤️" {
		t.Errorf("body = %q", cm.Parts[0].Content.Body)
	}
}

func TestInstagramEncryptedPhoto(t *testing.T) {
	calls := fakeDownloads(t)
	cm, intent := convertIG(t, &MessageConverter{}, igPayload(igMediaContent(&instamadilloCoreTypeMedia.Media{
		Media: &instamadilloCoreTypeMedia.Media_StaticPhoto{StaticPhoto: &instamadilloCoreTypeMedia.StaticPhoto{
			MediaTransport: igRef("image/jpeg"), Width: ptr.Ptr(int32(640)), Height: ptr.Ptr(int32(480)),
		}},
	})))
	c := cm.Parts[0].Content
	if c.MsgType != event.MsgImage || c.Info.Width != 640 || c.Info.Height != 480 || c.Info.MimeType != "image/jpeg" || c.Body != "image.jpg" {
		t.Errorf("content = %+v info = %+v", c, c.Info)
	}
	if len(intent.uploads) != 1 || string(intent.uploads[0].data) != "fake file" {
		t.Errorf("uploads = %+v", intent.uploads)
	}
	if len(*calls) != 1 {
		t.Fatalf("downloads = %d", len(*calls))
	}
	got := (*calls)[0]
	if got.mediaType != whatsmeow.MediaImage || got.integral.GetDirectPath() != "/m1/v/t/fake-path" {
		t.Errorf("download = %+v", got)
	}
	if !bytes.Equal(got.integral.MediaKey, testKey) || !bytes.Equal(got.integral.FileSHA256, testHash) || !bytes.Equal(got.integral.FileEncSHA256, testHash) {
		t.Errorf("keys must be decoded whatever the encoding: %x %x %x", got.integral.MediaKey, got.integral.FileSHA256, got.integral.FileEncSHA256)
	}
}

func TestInstagramEncryptedVoiceVideoStickerGif(t *testing.T) {
	fakeDownloads(t)
	mc := &MessageConverter{}
	ctx, _ := waTestContext()

	voice := mc.igMedia(ctx, &instamadilloCoreTypeMedia.Media{Media: &instamadilloCoreTypeMedia.Media_Voice{
		Voice: &instamadilloCoreTypeMedia.Voice{MediaTransport: igRef("audio/ogg"), Duration: ptr.Ptr(int32(4200))},
	}})
	if voice.Content.MsgType != event.MsgAudio || voice.Content.MSC3245Voice == nil || voice.Content.Info.Duration != 4200 {
		t.Errorf("voice = %+v", voice.Content)
	}
	video := mc.igMedia(ctx, &instamadilloCoreTypeMedia.Media{Media: &instamadilloCoreTypeMedia.Media_Video{
		Video: &instamadilloCoreTypeMedia.Video{MediaTransport: igRef("video/mp4"), Width: ptr.Ptr(int32(1280)), Height: ptr.Ptr(int32(720))},
	}})
	if video.Content.MsgType != event.MsgVideo || video.Content.Info.Width != 1280 || video.Content.Body != "video.mp4" {
		t.Errorf("video = %+v", video.Content)
	}
	sticker := mc.igMedia(ctx, &instamadilloCoreTypeMedia.Media{Media: &instamadilloCoreTypeMedia.Media_Gif{
		Gif: &instamadilloCoreTypeMedia.Gif{MediaTransport: igRef("image/webp"), IsSticker: ptr.Ptr(true)},
	}})
	if sticker.Type != event.EventSticker {
		t.Errorf("a gif marked as sticker must be a sticker: %v", sticker.Type)
	}
	avatar := mc.igMedia(ctx, &instamadilloCoreTypeMedia.Media{Media: &instamadilloCoreTypeMedia.Media_AvatarSticker{
		AvatarSticker: &instamadilloCoreTypeMedia.AvatarSticker{MediaTransport: igRef("image/webp")},
	}})
	if avatar.Type != event.EventSticker {
		t.Errorf("avatar sticker type = %v", avatar.Type)
	}
	gif := mc.igMedia(ctx, &instamadilloCoreTypeMedia.Media{Media: &instamadilloCoreTypeMedia.Media_Gif{
		Gif: &instamadilloCoreTypeMedia.Gif{MediaTransport: igRef("video/mp4")},
	}})
	if gif.Content.MsgType != event.MsgVideo || gif.Extra["info"] == nil {
		t.Errorf("gif = %+v %+v", gif.Content, gif.Extra)
	}
}

func TestInstagramEncryptedMediaWithoutPathIsAnError(t *testing.T) {
	ctx, _ := waTestContext()
	part := (&MessageConverter{}).igMedia(ctx, &instamadilloCoreTypeMedia.Media{Media: &instamadilloCoreTypeMedia.Media_StaticPhoto{
		StaticPhoto: &instamadilloCoreTypeMedia.StaticPhoto{MediaTransport: &instamadilloCoreTypeMedia.CommonMediaTransport{}},
	}})
	if part.Content.MsgType != event.MsgNotice || !strings.Contains(part.Content.Body, "Failed to transfer media") {
		t.Errorf("content = %+v", part.Content)
	}
}

func TestInstagramEncryptedViewOnce(t *testing.T) {
	fakeDownloads(t)
	raven := &instamadilloCoreTypeMedia.Media{Media: &instamadilloCoreTypeMedia.Media_Raven{Raven: &instamadilloCoreTypeMedia.Raven{
		ViewMode: instamadilloCoreTypeMedia.Raven_RAVEN_VIEW_MODEL_ONCE.Enum(),
		Content: &instamadilloCoreTypeMedia.RavenContent{RavenContent: &instamadilloCoreTypeMedia.RavenContent_StaticPhoto{
			StaticPhoto: &instamadilloCoreTypeMedia.StaticPhoto{MediaTransport: igRef("image/jpeg")},
		}},
	}}}
	ctx, _ := waTestContext()
	if part := (&MessageConverter{}).igMedia(ctx, raven); part.Content.MsgType != event.MsgImage {
		t.Errorf("view once media is bridged unless disabled: %+v", part.Content)
	}
	part := (&MessageConverter{DisableViewOnce: true}).igMedia(ctx, raven)
	if part.Content.MsgType != event.MsgNotice || !strings.Contains(part.Content.Body, "viewed") {
		t.Errorf("disabled view once = %+v", part.Content)
	}
}

func TestInstagramEncryptedCollectionIsAnAlbum(t *testing.T) {
	fakeDownloads(t)
	photo := func() *instamadilloCoreTypeMedia.Media {
		return &instamadilloCoreTypeMedia.Media{Media: &instamadilloCoreTypeMedia.Media_StaticPhoto{
			StaticPhoto: &instamadilloCoreTypeMedia.StaticPhoto{MediaTransport: igRef("image/jpeg")},
		}}
	}
	cm, _ := convertIG(t, &MessageConverter{}, igPayload(&instamadilloAddMessage.AddMessageContent{
		AddMessageContent: &instamadilloAddMessage.AddMessageContent_Collection{Collection: &instamadilloCoreTypeCollection.Collection{
			Name: ptr.Ptr("Trip"), Media: []*instamadilloCoreTypeMedia.Media{photo(), photo()},
		}},
	}))
	if len(cm.Parts) != 3 {
		t.Fatalf("parts = %d, want 2 photos and the caption", len(cm.Parts))
	}
	if info := album.Get(cm.Parts[0].Extra); info == nil || info.Count != 2 {
		t.Errorf("photos must share an album: %+v", cm.Parts[0].Extra)
	}
	if cm.Parts[2].Content.Body != "Trip" {
		t.Errorf("caption = %+v", cm.Parts[2].Content)
	}
}

func TestInstagramEncryptedLinkPreview(t *testing.T) {
	fakeDownloads(t)
	cm, _ := convertIG(t, &MessageConverter{}, igPayload(&instamadilloAddMessage.AddMessageContent{
		AddMessageContent: &instamadilloAddMessage.AddMessageContent_Link{Link: &instamadilloCoreTypeLink.Link{
			Text: ptr.Ptr("read this"),
			LinkContext: &instamadilloCoreTypeLink.LinkContext{
				LinkURL:              ptr.Ptr("https://example.test/article"),
				LinkPreviewTitle:     ptr.Ptr("An article"),
				LinkSummary:          ptr.Ptr("What it says"),
				LinkPreviewThumbnail: &instamadilloCoreTypeMedia.Thumbnail{MediaTransport: igRef("image/jpeg"), Width: ptr.Ptr(int32(100)), Height: ptr.Ptr(int32(50))},
			},
		}},
	}))
	c := cm.Parts[0].Content
	if c.Body != "read this\n\nhttps://example.test/article" {
		t.Errorf("body = %q", c.Body)
	}
	if len(c.BeeperLinkPreviews) != 1 {
		t.Fatalf("previews = %+v", c.BeeperLinkPreviews)
	}
	p := c.BeeperLinkPreviews[0]
	if p.MatchedURL != "https://example.test/article" || p.Title != "An article" || p.Description != "What it says" || p.ImageURL == "" || p.ImageWidth != 100 {
		t.Errorf("preview = %+v", p)
	}
}

func TestInstagramEncryptedSharedPost(t *testing.T) {
	fakeDownloads(t)
	cm, _ := convertIG(t, &MessageConverter{}, igPayload(&instamadilloAddMessage.AddMessageContent{
		AddMessageContent: &instamadilloAddMessage.AddMessageContent_ReceiverFetchXma{ReceiverFetchXma: &instamadilloAddMessage.ReceiverFetchXma{
			XmaContentRef: &instamadilloXmaContentRef.XmaContentRef{
				ContentType: instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_CLIP.Enum(),
				TargetURL:   ptr.Ptr("https://example.test/reel/abc"),
				UserName:    ptr.Ptr("someone"),
			},
		}},
	}))
	body := cm.Parts[0].Content.Body
	if body != "Shared a reel from someone\n\nhttps://example.test/reel/abc" {
		t.Errorf("body = %q", body)
	}
}

func TestInstagramEncryptedNoticesAndPlaceholders(t *testing.T) {
	cm, _ := convertIG(t, &MessageConverter{}, igPayload(&instamadilloAddMessage.AddMessageContent{
		AddMessageContent: &instamadilloAddMessage.AddMessageContent_Placeholder{Placeholder: &instamadilloAddMessage.Placeholder{
			PlaceholderType: instamadilloAddMessage.Placeholder_PLACEHOLDER_TYPE_DECRYPTION_FAILURE.Enum(),
		}},
	}))
	if c := cm.Parts[0].Content; c.MsgType != event.MsgNotice || !strings.Contains(c.Body, "couldn't be decrypted") {
		t.Errorf("placeholder = %+v", c)
	}
	cm, _ = convertIG(t, &MessageConverter{}, igPayload(&instamadilloAddMessage.AddMessageContent{
		AddMessageContent: &instamadilloAddMessage.AddMessageContent_AdminMessage{AdminMessage: &instamadilloCoreTypeAdminMessage.AdminMessage{
			AdminMessageSubtype: &instamadilloCoreTypeAdminMessage.AdminMessage_DeviceAdminMessage{DeviceAdminMessage: &instamadilloCoreTypeAdminMessage.DeviceAdminMessage{
				DeviceAdminMessageType: instamadilloCoreTypeAdminMessage.DeviceAdminMessage_DEVICE_ADMIN_MESSAGE_TYPE_SECURITY_ALERT_PARTICIPANT_NEW_LOGIN.Enum(),
				DeviceName:             ptr.Ptr("Test Phone"),
			}},
		}},
	}))
	if c := cm.Parts[0].Content; c.MsgType != event.MsgNotice || !strings.Contains(c.Body, "Test Phone") {
		t.Errorf("admin = %+v", c)
	}
	cm, _ = convertIG(t, &MessageConverter{}, igPayload(&instamadilloAddMessage.AddMessageContent{
		AddMessageContent: &instamadilloAddMessage.AddMessageContent_ActionLog{ActionLog: &instamadilloCoreTypeActionLog.ActionLog{
			ActionLogSubtype: &instamadilloCoreTypeActionLog.ActionLog_ActionLogReaction{ActionLogReaction: &instamadilloCoreTypeActionLog.ActionLogReaction{EmojiUnicode: ptr.Ptr("😂")}},
		}},
	}))
	if !cm.Parts[0].DontBridge {
		t.Error("the log line of a reaction must not be bridged, the reaction is")
	}
}

func fakeRecent(ids ...string) func(context.Context, networkid.PortalKey, int) ([]*database.Message, error) {
	return func(context.Context, networkid.PortalKey, int) ([]*database.Message, error) {
		var out []*database.Message
		for _, id := range ids {
			out = append(out, &database.Message{ID: networkid.MessageID(id)})
		}
		return out, nil
	}
}

func TestInstagramEncryptedReplyFindsTheMessage(t *testing.T) {
	target := string(metaid.MakeWAMessageID(testIGChat, waTypes.NewJID("100000000000003", waTypes.MessengerServer), "REPLIEDTOID9"))
	mc := &MessageConverter{RecentMessages: fakeRecent("fb:mid.other", "wa:junk", string(metaid.MakeWAMessageID(testIGChat, testIGSender, "SOMETHINGELSE")), target)}
	payload := igPayload(igTextContent("agreed"))
	payload.Metadata = &instamadilloAddMessage.AddMessageMetadata{
		RepliedToMessage: &instamadilloAddMessage.RepliedToMessage{RepliedToMessageOtid: ptr.Ptr("REPLIEDTOID9")},
	}
	cm, _ := convertIG(t, mc, payload)
	if cm.ReplyTo == nil || string(cm.ReplyTo.MessageID) != target {
		t.Errorf("reply = %+v, want %s", cm.ReplyTo, target)
	}
	payload.Metadata.RepliedToMessage.RepliedToMessageOtid = ptr.Ptr("UNKNOWNID")
	if cm, _ = convertIG(t, mc, payload); cm.ReplyTo != nil {
		t.Errorf("a reply to a message we don't have must not link anything: %+v", cm.ReplyTo)
	}
}

func TestFindWAMessageWithoutDatabase(t *testing.T) {
	if got := (&MessageConverter{}).FindWAMessage(context.Background(), networkid.PortalKey{}, "X"); got != "" {
		t.Errorf("got %q", got)
	}
	mc := &MessageConverter{RecentMessages: fakeRecent(string(metaid.MakeWAMessageID(testIGChat, testIGSender, "ABC")))}
	if got := mc.FindWAMessage(context.Background(), networkid.PortalKey{}, "ABC"); got == "" {
		t.Error("message not found")
	}
	if got := mc.FindWAMessage(context.Background(), networkid.PortalKey{}, ""); got != "" {
		t.Errorf("an empty ID matches nothing, got %q", got)
	}
}

func TestDecodeIGBinary(t *testing.T) {
	want := bytes.Repeat([]byte{0xfb, 0x01}, 16)
	for name, in := range map[string]string{
		"raw":       string(want),
		"hex":       hex.EncodeToString(want),
		"base64":    base64.StdEncoding.EncodeToString(want),
		"base64url": base64.URLEncoding.EncodeToString(want),
		"rawbase64": base64.RawStdEncoding.EncodeToString(want),
	} {
		if got := decodeIGBinary(in, 32); !bytes.Equal(got, want) {
			t.Errorf("%s: got %x", name, got)
		}
	}
	if decodeIGBinary("", 32) != nil {
		t.Error("empty stays empty")
	}
}
