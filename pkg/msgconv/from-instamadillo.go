package msgconv

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"go.mau.fi/util/exmime"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/instamadilloAddMessage"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeAdminMessage"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeCollection"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeLink"
	"go.mau.fi/whatsmeow/proto/instamadilloCoreTypeMedia"
	"go.mau.fi/whatsmeow/proto/instamadilloXmaContentRef"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waMediaTransport"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/album"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/msgconv/mediadl"
	"go.mau.fi/mautrix-meta/pkg/msgconv/textfmt"
)

// decodeIGBinary reads a binary value that an encrypted Instagram media reference carries in a string field.
// The field is a string in the schema; it holds the raw bytes, hex or base64, so all three are accepted and
// the expected length tells them apart.
func decodeIGBinary(value string, length int) []byte {
	switch {
	case value == "":
		return nil
	case len(value) == length:
		return []byte(value)
	case len(value) == length*2:
		if out, err := hex.DecodeString(value); err == nil {
			return out
		}
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if out, err := enc.DecodeString(value); err == nil && len(out) > 0 {
			return out
		}
	}
	return []byte(value)
}

// igTransport is an Instagram media reference as the WhatsApp media transport that downloads it.
func igTransport(ref *instamadilloCoreTypeMedia.CommonMediaTransport) (*waMediaTransport.WAMediaTransport, error) {
	if ref.GetDirectPath() == "" {
		return nil, fmt.Errorf("media has no download path")
	}
	integral := &waMediaTransport.WAMediaTransport_Integral{
		FileSHA256:    decodeIGBinary(ref.GetFileSHA256(), 32),
		MediaKey:      decodeIGBinary(ref.GetMediaKey(), 32),
		FileEncSHA256: decodeIGBinary(ref.GetFileEncSHA256(), 32),
		DirectPath:    ref.DirectPath,
	}
	if ts, err := strconv.ParseInt(ref.GetMediaKeyTimestamp(), 10, 64); err == nil {
		integral.MediaKeyTimestamp = &ts
	}
	length := uint64(ref.GetFileLength())
	return &waMediaTransport.WAMediaTransport{
		Integral:  integral,
		Ancillary: &waMediaTransport.WAMediaTransport_Ancillary{FileLength: &length, Mimetype: ref.Mimetype},
	}, nil
}

func (mc *MessageConverter) igReupload(
	ctx context.Context,
	ref *instamadilloCoreTypeMedia.CommonMediaTransport,
	mediaType whatsmeow.MediaType,
	baseName, defaultMime string,
) (*bridgev2.ConvertedMessagePart, error) {
	transport, err := igTransport(ref)
	if err != nil {
		return nil, err
	}
	if transport.Ancillary.GetMimetype() == "" {
		transport.Ancillary.Mimetype = &defaultMime
	}
	return mc.reuploadWhatsAppAttachment(ctx, transport, mediaType, func(ctx context.Context, data []byte, mimeType string) ([]byte, string, string, error) {
		return data, mimeType, baseName + exmime.ExtensionFromMimetype(mimeType), nil
	})
}

func (mc *MessageConverter) igPhoto(ctx context.Context, photo *instamadilloCoreTypeMedia.StaticPhoto) (*bridgev2.ConvertedMessagePart, error) {
	part, err := mc.igReupload(ctx, photo.GetMediaTransport(), whatsmeow.MediaImage, "image", "image/jpeg")
	if err != nil {
		return nil, err
	}
	part.Content.MsgType = event.MsgImage
	part.Content.Info.Width = int(photo.GetWidth())
	part.Content.Info.Height = int(photo.GetHeight())
	return part, nil
}

func (mc *MessageConverter) igVideo(ctx context.Context, video *instamadilloCoreTypeMedia.Video) (*bridgev2.ConvertedMessagePart, error) {
	part, err := mc.igReupload(ctx, video.GetMediaTransport(), whatsmeow.MediaVideo, "video", "video/mp4")
	if err != nil {
		return nil, err
	}
	part.Content.MsgType = event.MsgVideo
	part.Content.Info.Width = int(video.GetWidth())
	part.Content.Info.Height = int(video.GetHeight())
	return part, nil
}

func (mc *MessageConverter) igVoice(ctx context.Context, voice *instamadilloCoreTypeMedia.Voice) (*bridgev2.ConvertedMessagePart, error) {
	part, err := mc.igReupload(ctx, voice.GetMediaTransport(), whatsmeow.MediaAudio, "audio", "audio/ogg")
	if err != nil {
		return nil, err
	}
	part.Content.MsgType = event.MsgAudio
	part.Content.Info.Duration = int(voice.GetDuration())
	part.Content.MSC3245Voice = &event.MSC3245Voice{}
	part.Content.MSC1767Audio = &event.MSC1767Audio{Duration: part.Content.Info.Duration, Waveform: []int{}}
	return part, nil
}

func (mc *MessageConverter) igGif(ctx context.Context, gif *instamadilloCoreTypeMedia.Gif) (*bridgev2.ConvertedMessagePart, error) {
	part, err := mc.igReupload(ctx, gif.GetMediaTransport(), whatsmeow.MediaImage, "gif", "image/gif")
	if err != nil {
		return nil, err
	}
	part.Content.Info.Width = int(gif.GetWidth())
	part.Content.Info.Height = int(gif.GetHeight())
	if gif.GetIsSticker() {
		part.Type = event.EventSticker
	} else if strings.HasPrefix(part.Content.Info.MimeType, "video/") {
		part.Content.MsgType = event.MsgVideo
		part.Extra["info"] = map[string]any{
			"fi.mau.gif":           true,
			"fi.mau.loop":          true,
			"fi.mau.autoplay":      true,
			"fi.mau.hide_controls": true,
			"fi.mau.no_audio":      true,
		}
	} else {
		part.Content.MsgType = event.MsgImage
	}
	return part, nil
}

func (mc *MessageConverter) igAvatarSticker(ctx context.Context, sticker *instamadilloCoreTypeMedia.AvatarSticker) (*bridgev2.ConvertedMessagePart, error) {
	part, err := mc.igReupload(ctx, sticker.GetMediaTransport(), whatsmeow.MediaImage, "sticker", "image/webp")
	if err != nil {
		return nil, err
	}
	part.Type = event.EventSticker
	return part, nil
}

func (mc *MessageConverter) igRaven(ctx context.Context, raven *instamadilloCoreTypeMedia.Raven) (*bridgev2.ConvertedMessagePart, error) {
	kind := "photo"
	if raven.GetContent().GetVideo() != nil {
		kind = "video"
	}
	viewMode := raven.GetViewMode()
	if mc.DisableViewOnce && (viewMode == instamadilloCoreTypeMedia.Raven_RAVEN_VIEW_MODEL_ONCE || viewMode == instamadilloCoreTypeMedia.Raven_RAVEN_VIEW_MODEL_REPLAYABLE) {
		viewed := "viewed"
		if viewMode == instamadilloCoreTypeMedia.Raven_RAVEN_VIEW_MODEL_REPLAYABLE {
			viewed = "replayed"
		}
		return mc.makeViewOnceError(ctx, kind, viewed), nil
	}
	if video := raven.GetContent().GetVideo(); video != nil {
		return mc.igVideo(ctx, video)
	} else if photo := raven.GetContent().GetStaticPhoto(); photo != nil {
		return mc.igPhoto(ctx, photo)
	}
	return nil, fmt.Errorf("disappearing media has no content")
}

// igMedia converts one media item of an encrypted Instagram message.
func (mc *MessageConverter) igMedia(ctx context.Context, media *instamadilloCoreTypeMedia.Media) *bridgev2.ConvertedMessagePart {
	var part *bridgev2.ConvertedMessagePart
	var err error
	switch typed := media.GetMedia().(type) {
	case *instamadilloCoreTypeMedia.Media_StaticPhoto:
		part, err = mc.igPhoto(ctx, typed.StaticPhoto)
	case *instamadilloCoreTypeMedia.Media_Video:
		part, err = mc.igVideo(ctx, typed.Video)
	case *instamadilloCoreTypeMedia.Media_Voice:
		part, err = mc.igVoice(ctx, typed.Voice)
	case *instamadilloCoreTypeMedia.Media_Gif:
		part, err = mc.igGif(ctx, typed.Gif)
	case *instamadilloCoreTypeMedia.Media_AvatarSticker:
		part, err = mc.igAvatarSticker(ctx, typed.AvatarSticker)
	case *instamadilloCoreTypeMedia.Media_Raven:
		part, err = mc.igRaven(ctx, typed.Raven)
	default:
		return noticePart(fmt.Sprintf("Unsupported message (%T)\n\nPlease open in the %s", typed, appName(ctx)))
	}
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to convert encrypted Instagram media")
		return wrapError("Failed to transfer media", err)
	}
	return part
}

func (mc *MessageConverter) igCollection(ctx context.Context, collection *instamadilloCoreTypeCollection.Collection) []*bridgev2.ConvertedMessagePart {
	parts := make([]*bridgev2.ConvertedMessagePart, 0, len(collection.GetMedia())+1)
	for _, media := range collection.GetMedia() {
		parts = append(parts, mc.igMedia(ctx, media))
	}
	if msgID, ok := ctx.Value(mediadl.ContextKeyMsgID).(networkid.MessageID); ok {
		album.Tag(parts, AlbumID(msgID))
	}
	if name := collection.GetName(); name != "" {
		parts = append(parts, &bridgev2.ConvertedMessagePart{
			Type:    event.EventMessage,
			Content: &event.MessageEventContent{MsgType: event.MsgText, Body: name},
		})
	}
	return parts
}

func (mc *MessageConverter) igLink(ctx context.Context, link *instamadilloCoreTypeLink.Link) *bridgev2.ConvertedMessagePart {
	linkCtx := link.GetLinkContext()
	url := linkCtx.GetLinkURL()
	body := link.GetText()
	if url != "" && !strings.Contains(body, url) {
		if body == "" {
			body = url
		} else {
			body += "\n\n" + url
		}
	}
	part := mc.igText(ctx, body)
	if url == "" {
		return part
	}
	preview := &event.BeeperLinkPreview{
		MatchedURL: url,
		LinkPreview: event.LinkPreview{
			CanonicalURL: url,
			Title:        linkCtx.GetLinkPreviewTitle(),
			Description:  linkCtx.GetLinkSummary(),
		},
	}
	if preview.Description == "" {
		preview.Description = linkCtx.GetLinkPreviewBody()
	}
	if thumb := linkCtx.GetLinkPreviewThumbnail(); thumb.GetMediaTransport().GetDirectPath() != "" {
		if transport, err := igTransport(thumb.GetMediaTransport()); err == nil {
			if data, err := downloadWAMedia(ctx, transport.GetIntegral(), whatsmeow.MediaImage); err != nil {
				zerolog.Ctx(ctx).Err(err).Msg("Failed to download encrypted Instagram link preview thumbnail")
			} else {
				uploadPreviewImage(ctx, data, int(thumb.GetWidth()), int(thumb.GetHeight()), preview)
			}
		}
	}
	part.Content.BeeperLinkPreviews = []*event.BeeperLinkPreview{preview}
	return part
}

func (mc *MessageConverter) igText(ctx context.Context, text string) *bridgev2.ConvertedMessagePart {
	return &bridgev2.ConvertedMessagePart{
		Type:    event.EventMessage,
		Content: textfmt.MetaToMatrixText(ctx, text, nil, mc.getBasicUserInfo),
	}
}

var igContentTypeNames = map[instamadilloXmaContentRef.ReceiverFetchContentType]string{
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_NOTE:            "a note",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_STORY:           "a story",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_PROFILE:         "a profile",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_CLIP:            "a reel",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_FEED:            "a post",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_LIVE:            "a live video",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_COMMENT:         "a comment",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_LOCATION_SHARE:  "a location",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_REELS_AUDIO:     "reels audio",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_MEDIA_NOTE:      "a note",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_STORY_HIGHLIGHT: "a story highlight",
	instamadilloXmaContentRef.ReceiverFetchContentType_RECEIVER_FETCH_CONTENT_TYPE_SOCIAL_CONTEXT:  "a post",
}

// igReceiverFetch is a shared post, reel, story or profile. The content itself is fetched by the receiving
// app, so all the bridge has is the link and whatever text and preview picture came along.
func (mc *MessageConverter) igReceiverFetch(ctx context.Context, xma *instamadilloAddMessage.ReceiverFetchXma) []*bridgev2.ConvertedMessagePart {
	ref := xma.GetXmaContentRef()
	body := xma.GetText()
	url := ref.GetTargetURL()
	if body == "" {
		body = "Shared " + igContentTypeNames[ref.GetContentType()]
		if body == "Shared " {
			body = "Shared something"
		}
		if user := ref.GetUserName(); user != "" {
			body += " from " + user
		}
	}
	if url != "" && !strings.Contains(body, url) {
		body += "\n\n" + url
	}
	parts := []*bridgev2.ConvertedMessagePart{{
		Type:    event.EventMessage,
		Content: &event.MessageEventContent{MsgType: event.MsgText, Body: body},
	}}
	if xma.GetMedia() != nil {
		parts = append([]*bridgev2.ConvertedMessagePart{mc.igMedia(ctx, xma.GetMedia())}, parts...)
	}
	return parts
}

func igAdminMessage(admin *instamadilloCoreTypeAdminMessage.AdminMessage) string {
	device := admin.GetDeviceAdminMessage()
	name := device.GetDeviceName()
	switch device.GetDeviceAdminMessageType() {
	case instamadilloCoreTypeAdminMessage.DeviceAdminMessage_DEVICE_ADMIN_MESSAGE_TYPE_LOCAL_USER_CHANGED_IDENTITY_KEY_NAMED_DEVICE:
		if name != "" {
			return "You signed in on a new device: " + name
		}
		return "You signed in on a new device"
	case instamadilloCoreTypeAdminMessage.DeviceAdminMessage_DEVICE_ADMIN_MESSAGE_TYPE_SECURITY_ALERT_PARTICIPANT_KEY_CHANGE:
		return "The security code of this chat changed"
	case instamadilloCoreTypeAdminMessage.DeviceAdminMessage_DEVICE_ADMIN_MESSAGE_TYPE_SECURITY_ALERT_PARTICIPANT_NEW_LOGIN:
		if name != "" {
			return "A participant signed in on a new device: " + name
		}
		return "A participant signed in on a new device"
	}
	return "Encryption settings of this chat changed"
}

func igPlaceholder(ctx context.Context, kind instamadilloAddMessage.Placeholder_Type) string {
	switch kind {
	case instamadilloAddMessage.Placeholder_PLACEHOLDER_TYPE_DECRYPTION_FAILURE:
		return "A message couldn't be decrypted. The sender's device may still send it again."
	case instamadilloAddMessage.Placeholder_PLACEHOLDER_TYPE_NOT_SUPPORTED_NEED_UPDATE:
		return "The sender used something this version of the Instagram app doesn't support"
	case instamadilloAddMessage.Placeholder_PLACEHOLDER_TYPE_DEVICE_UNAVAILABLE:
		return "A message was sent while this device was unavailable"
	}
	return fmt.Sprintf("Unsupported message\n\nPlease open in the %s", appName(ctx))
}

// instamadilloToMatrix converts an encrypted Instagram message: text, links, shared posts, all kinds of
// media, likes, and the notices Instagram puts in chats.
func (mc *MessageConverter) instamadilloToMatrix(ctx context.Context, payload *instamadilloAddMessage.AddMessagePayload) (parts []*bridgev2.ConvertedMessagePart, replyOverride *waCommon.MessageKey) {
	switch content := payload.GetContent().GetAddMessageContent().(type) {
	case *instamadilloAddMessage.AddMessageContent_Text:
		parts = []*bridgev2.ConvertedMessagePart{mc.igText(ctx, content.Text.GetText())}
	case *instamadilloAddMessage.AddMessageContent_Like:
		parts = []*bridgev2.ConvertedMessagePart{mc.igText(ctx, "❤️")}
	case *instamadilloAddMessage.AddMessageContent_Link:
		parts = []*bridgev2.ConvertedMessagePart{mc.igLink(ctx, content.Link)}
	case *instamadilloAddMessage.AddMessageContent_ReceiverFetchXma:
		parts = mc.igReceiverFetch(ctx, content.ReceiverFetchXma)
	case *instamadilloAddMessage.AddMessageContent_Media:
		parts = []*bridgev2.ConvertedMessagePart{mc.igMedia(ctx, content.Media)}
	case *instamadilloAddMessage.AddMessageContent_Collection:
		parts = mc.igCollection(ctx, content.Collection)
	case *instamadilloAddMessage.AddMessageContent_Placeholder:
		parts = []*bridgev2.ConvertedMessagePart{noticePart(igPlaceholder(ctx, content.Placeholder.GetPlaceholderType()))}
	case *instamadilloAddMessage.AddMessageContent_AdminMessage:
		parts = []*bridgev2.ConvertedMessagePart{noticePart(igAdminMessage(content.AdminMessage))}
	case *instamadilloAddMessage.AddMessageContent_ActionLog:
		// A reaction is also sent as a reaction event, so this line about it is not shown.
		part := noticePart("Reacted to a message")
		part.DontBridge = true
		parts = []*bridgev2.ConvertedMessagePart{part}
	default:
		zerolog.Ctx(ctx).Warn().Type("content_type", content).Msg("Unrecognized encrypted Instagram message content type")
		parts = []*bridgev2.ConvertedMessagePart{noticePart(fmt.Sprintf("Unsupported message (%T)\n\nPlease open in the %s", content, appName(ctx)))}
	}
	if payload.GetMetadata().GetSendSilently() {
		for _, part := range parts {
			if part.Content != nil && part.Content.Mentions != nil {
				part.Content.Mentions.Room = false
			}
		}
	}
	return parts, nil
}

// igReplyTarget is the message an encrypted Instagram message replies to. The reply names it by the ID the
// sender gave it, which is the ID of the message itself, so it is found among the chat's recent messages.
func (mc *MessageConverter) igReplyTarget(ctx context.Context, portal networkid.PortalKey, payload *instamadilloAddMessage.AddMessagePayload) *networkid.MessageOptionalPartID {
	otid := payload.GetMetadata().GetRepliedToMessage().GetRepliedToMessageOtid()
	if otid == "" {
		return nil
	}
	if id := mc.FindWAMessage(ctx, portal, otid); id != "" {
		return &networkid.MessageOptionalPartID{MessageID: id}
	}
	return nil
}

// matchWAMessageID is the ID of the message that a sender-given ID belongs to. IDs of encrypted messages are
// made of the chat, the sender and the ID the sender gave the message.
func matchWAMessageID(messages []*database.Message, externalID string) networkid.MessageID {
	for _, msg := range messages {
		if parsed, ok := metaid.ParseMessageID(msg.ID).(metaid.ParsedWAMessageID); ok && parsed.ID == externalID {
			return msg.ID
		}
	}
	return ""
}

// FindWAMessage finds a message of an encrypted chat by the ID its sender gave it, when it is one of the
// chat's latest messages. It is used where a message is named without its sender: replies, reactions, edits
// and deletions of encrypted Instagram messages.
func (mc *MessageConverter) FindWAMessage(ctx context.Context, portal networkid.PortalKey, externalID string) networkid.MessageID {
	if externalID == "" {
		return ""
	}
	recent := mc.RecentMessages
	if recent == nil && mc.Bridge != nil {
		recent = mc.Bridge.DB.Message.GetLastNInPortal
	}
	if recent == nil {
		return ""
	}
	messages, err := recent(ctx, portal, recentMessageLookback)
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Str("external_id", externalID).Msg("Failed to look up recent messages")
		return ""
	}
	return matchWAMessageID(messages, externalID)
}

const recentMessageLookback = 1000
