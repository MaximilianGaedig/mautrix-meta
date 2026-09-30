package textfmt

import (
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
)

const forwardedNotice = "↷ Forwarded"

// MarkForwarded labels a forwarded message as forwarded, the way the WhatsApp bridge does: a line above the text,
// or the whole body of a media message that has no caption. Only the first part is labelled, and only when
// it is text or media.
func MarkForwarded(parts []*bridgev2.ConvertedMessagePart) {
	for _, part := range parts {
		if part.DontBridge || part.Content == nil {
			continue
		}
		markContentForwarded(part.Content)
		return
	}
}

func markContentForwarded(content *event.MessageEventContent) {
	isMedia := content.MsgType.IsMedia()
	isText := content.MsgType.IsText()
	hasCaption := content.FileName != "" && content.FileName != content.Body
	if isMedia && !hasCaption {
		content.FileName = content.Body
		content.Body = forwardedNotice
		content.Format = event.FormatHTML
		content.FormattedBody = "<p data-mx-forwarded-notice><em>" + forwardedNotice + "</em></p>"
	} else if isText || isMedia {
		content.EnsureHasHTML()
		content.Body = forwardedNotice + "\n\n" + content.Body
		content.FormattedBody = "<p data-mx-forwarded-notice><em>" + forwardedNotice + "</em></p>" + content.FormattedBody
	}
}
