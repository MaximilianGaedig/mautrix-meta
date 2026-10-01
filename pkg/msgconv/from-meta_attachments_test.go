package msgconv

import (
	"context"
	"fmt"
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/album"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// directMediaMatrix hands out content URIs without a homeserver, as direct media does.
type directMediaMatrix struct{ bridgev2.MatrixConnector }

func (directMediaMatrix) GenerateContentURI(_ context.Context, mediaID networkid.MediaID) (id.ContentURIString, error) {
	return id.ContentURIString(fmt.Sprintf("mxc://example.test/%x", []byte(mediaID))), nil
}

// A Messenger message with several attachments comes as one message row and one attachment row
// each. Every one of them has to reach Matrix, as its own event, marked as one album.
func TestMessengerMessageWithSeveralAttachments(t *testing.T) {
	const msgID = "mid.$several"
	blob := func(fbid string) *table.LSInsertBlobAttachment {
		return &table.LSInsertBlobAttachment{
			MessageId: msgID, AttachmentFbid: fbid, AttachmentType: table.AttachmentTypeImage,
			Filename: fbid + ".jpg", PreviewUrl: "https://scontent.example/" + fbid, PreviewUrlMimeType: "image/jpeg",
		}
	}
	tbl := &table.LSTable{
		LSInsertMessage: []*table.LSInsertMessage{{MessageId: msgID, ThreadKey: 1, Text: "three photos and a clip"}},
		// Messenger sometimes repeats an attachment row; the repeat is not a fourth photo.
		LSInsertBlobAttachment: []*table.LSInsertBlobAttachment{blob("1"), blob("2"), blob("3"), blob("1")},
		LSInsertAttachment: []*table.LSInsertAttachment{{
			MessageId: msgID, AttachmentFbid: "4", AttachmentType: table.AttachmentTypeVideo,
			Filename: "clip.mp4", PlayableUrl: "https://video.example/4", PlayableUrlMimeType: "video/mp4",
		}},
	}
	_, insert := tbl.WrapMessages()
	if len(insert) != 1 {
		t.Fatalf("wrapped %d messages, want 1", len(insert))
	}

	mc := &MessageConverter{DirectMedia: true}
	portal := &bridgev2.Portal{
		Portal: &database.Portal{PortalKey: networkid.PortalKey{ID: "1", Receiver: "1000"}},
		Bridge: &bridgev2.Bridge{Matrix: directMediaMatrix{}},
	}
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "1000"}}
	cm := mc.ToMatrix(context.Background(), portal, nil, login, nil, networkid.MessageID(msgID), insert[0])

	var media []*bridgev2.ConvertedMessagePart
	var texts []string
	for _, part := range cm.Parts {
		if album.IsMediaPart(part) {
			media = append(media, part)
		} else {
			texts = append(texts, part.Content.Body)
		}
	}
	if len(media) != 4 || len(texts) != 1 || texts[0] != "three photos and a clip" {
		t.Fatalf("got %d media parts and texts %q, want 4 media parts and the caption", len(media), texts)
	}
	wantTypes := []event.MessageType{event.MsgImage, event.MsgImage, event.MsgImage, event.MsgVideo}
	urls, partIDs := map[id.ContentURIString]bool{}, map[networkid.PartID]bool{}
	for i, part := range media {
		info := album.Get(part.Extra)
		if part.Content.MsgType != wantTypes[i] || info == nil || info.ID != AlbumID(msgID) || info.Index != i || info.Count != 4 {
			t.Errorf("part %d: msgtype %s, album %+v", i, part.Content.MsgType, info)
		}
		urls[part.Content.URL] = true
		partIDs[part.ID] = true
	}
	if len(urls) != 4 || len(partIDs) != 4 {
		t.Errorf("the attachments must be four different files under four part IDs: %v %v", urls, partIDs)
	}
}
