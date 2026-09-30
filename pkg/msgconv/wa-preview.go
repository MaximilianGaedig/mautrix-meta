package msgconv

import (
	"bytes"
	"context"
	"image"
	"net/http"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/msgconv/mediadl"
)

// waLinkPreviews is the link preview of an encrypted text message. A message without a matched URL has none,
// which is an empty list rather than nil so that clients don't try to make one.
func (mc *MessageConverter) waLinkPreviews(ctx context.Context, msg *waConsumerApplication.ConsumerApplication_ExtendedTextMessage) []*event.BeeperLinkPreview {
	if msg.GetMatchedText() == "" {
		return []*event.BeeperLinkPreview{}
	}
	preview := &event.BeeperLinkPreview{
		MatchedURL: msg.GetMatchedText(),
		LinkPreview: event.LinkPreview{
			CanonicalURL: msg.GetCanonicalURL(),
			Title:        msg.GetTitle(),
			Description:  msg.GetDescription(),
		},
	}
	if msg.GetPreviewType() == waConsumerApplication.ConsumerApplication_ExtendedTextMessage_VIDEO {
		preview.Type = "video.other"
	}
	if msg.GetThumbnail() != nil {
		mc.waPreviewThumbnail(ctx, msg, preview)
	}
	return []*event.BeeperLinkPreview{preview}
}

func (mc *MessageConverter) waPreviewThumbnail(ctx context.Context, msg *waConsumerApplication.ConsumerApplication_ExtendedTextMessage, preview *event.BeeperLinkPreview) {
	log := zerolog.Ctx(ctx)
	thumbnail, err := msg.DecodeThumbnail()
	if err != nil {
		log.Err(err).Msg("Failed to decode link preview thumbnail")
		return
	}
	data, err := downloadWAMedia(ctx, thumbnail.GetIntegral().GetTransport().GetIntegral(), whatsmeow.MediaImage)
	if err != nil {
		log.Err(err).Msg("Failed to download link preview thumbnail")
		return
	}
	preview.ImageType = http.DetectContentType(data)
	preview.ImageSize = event.IntOrString(len(data))
	preview.ImageWidth = event.IntOrString(thumbnail.GetAncillary().GetWidth())
	preview.ImageHeight = event.IntOrString(thumbnail.GetAncillary().GetHeight())
	if preview.ImageWidth == 0 || preview.ImageHeight == 0 {
		if cfg, _, err := image.DecodeConfig(bytes.NewReader(data)); err == nil {
			preview.ImageWidth, preview.ImageHeight = event.IntOrString(cfg.Width), event.IntOrString(cfg.Height)
		}
	}
	intent := ctx.Value(mediadl.ContextKeyIntent).(bridgev2.MatrixAPI)
	portal := ctx.Value(mediadl.ContextKeyPortal).(*bridgev2.Portal)
	preview.ImageURL, preview.ImageEncryption, err = intent.UploadMedia(ctx, portal.MXID, data, "", preview.ImageType)
	if err != nil {
		log.Err(err).Msg("Failed to reupload link preview thumbnail")
		preview.ImageURL, preview.ImageEncryption = "", nil
		preview.ImageType, preview.ImageSize, preview.ImageWidth, preview.ImageHeight = "", 0, 0, 0
	}
}
