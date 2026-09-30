package connector

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func TestPollRowsOneRowPerVoter(t *testing.T) {
	polls := (&table.LSTable{
		LSAddPollForThread: []*table.LSAddPollForThread{{PollID: 900, ThreadKey: 40}},
		LSAddPollVote: []*table.LSAddPollVote{
			{OptionID: 12, PollID: 900, ContactID: 6, TimestampMS: 150},
			{OptionID: 11, PollID: 900, ContactID: 5, TimestampMS: 200},
			{OptionID: 13, PollID: 900, ContactID: 5, TimestampMS: 200},
		},
	}).WrapPolls()
	rows := pollRows(polls, nil)
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want one per voter", len(rows))
	}
	if rows[0].ContactID != 5 || rows[0].ThreadKey != 40 || !reflect.DeepEqual(rows[0].optionIDs(), []int64{11, 13}) || rows[0].maxTimestampMS() != 200 {
		t.Errorf("first row = %+v", rows[0])
	}
	if rows[1].ContactID != 6 || !reflect.DeepEqual(rows[1].Votes, []metaid.VoteRow{{OptionID: 12, TimestampMS: 150}}) {
		t.Errorf("second row = %+v", rows[1])
	}
}

func TestPollRowsForAClosedPoll(t *testing.T) {
	polls := (&table.LSTable{
		LSAddPollForThread: []*table.LSAddPollForThread{{PollID: 900, ThreadKey: 40, LastUpdateMessageEventType: table.PollEventQuestionClosed}},
	}).WrapPolls()
	rows := pollRows(polls, nil)
	if len(rows) != 1 || !rows[0].Closed || rows[0].ThreadKey != 40 {
		t.Errorf("rows = %+v", rows)
	}
}

func TestPollRowsAskForTheThreadOfVotesWithoutOne(t *testing.T) {
	polls := (&table.LSTable{
		LSAddPollVote: []*table.LSAddPollVote{
			{OptionID: 11, PollID: 900, ContactID: 5, TimestampMS: 1},
			{OptionID: 21, PollID: 901, ContactID: 5, TimestampMS: 1},
		},
	}).WrapPolls()
	rows := pollRows(polls, func(pollID int64) int64 {
		if pollID == 900 {
			return 40
		}
		return 0
	})
	if len(rows) != 1 || rows[0].Poll.PollID != 900 || rows[0].ThreadKey != 40 {
		t.Errorf("rows = %+v: a vote for a poll in no known thread must be dropped", rows)
	}
}

func TestPollVoteIDRoundTrip(t *testing.T) {
	id := makePollVoteID(900, 5, 1234, []int64{11, 13})
	pollID, contactID, ok := parsePollVoteID(id)
	if !ok || pollID != 900 || contactID != 5 {
		t.Errorf("parse(%q) = %d, %d, %v", id, pollID, contactID, ok)
	}
	if metaid.ParseMessageID(id) != nil {
		t.Error("a vote ID must not parse as a Messenger message ID")
	}
	if _, _, ok = parsePollVoteID(metaid.MakeFBMessageID("mid.x")); ok {
		t.Error("a message ID is not a vote ID")
	}
	if makePollVoteID(900, 5, 1234, []int64{11}) == makePollVoteID(900, 5, 1234, []int64{13}) {
		t.Error("different votes must have different IDs")
	}
}

func TestPollFromCreateResponse(t *testing.T) {
	card := &table.LSInsertXmaAttachment{MessageId: "mid.new", AttachmentFbid: "1", ThreadKey: 40, ListItemsId: 903, ListItemsDescriptionText: "Q?"}
	tbl := &table.LSTable{
		LSInsertMessage:       []*table.LSInsertMessage{{MessageId: "mid.new", ThreadKey: 40}},
		LSInsertXmaAttachment: []*table.LSInsertXmaAttachment{card},
		LSInsertAttachmentCta: []*table.LSInsertAttachmentCta{{AttachmentFbid: "1", Type_: "xma_poll_details_card", TargetId: 903}},
		LSAddPollForThread: []*table.LSAddPollForThread{
			{PollID: 903, ThreadKey: 40, LastUpdateMessageID: "mid.other", LastUpdateMessageEventType: table.PollEventQuestionCreation},
			// An older poll of another chat that the response happens to mention.
			{PollID: 950, ThreadKey: 41},
		},
		LSAddPollOption: []*table.LSAddPollOption{{OptionID: 61, PollID: 903, OptionText: "Red"}},
	}
	poll, msgID, err := pollFromCreateResponse(tbl, 40)
	if err != nil {
		t.Fatal(err)
	}
	if poll.PollID != 903 || msgID != "mid.new" || len(poll.Options) != 1 {
		t.Errorf("poll = %+v, message = %q: the message with the poll card is the poll's message", poll, msgID)
	}

	// Without the card, the creation row names the message.
	tbl.LSInsertMessage, tbl.LSInsertXmaAttachment, tbl.LSInsertAttachmentCta = nil, nil, nil
	if _, msgID, err = pollFromCreateResponse(tbl, 40); err != nil || msgID != "mid.other" {
		t.Errorf("message = %q, err = %v", msgID, err)
	}

	tbl.LSAddPollForThread = nil
	if _, _, err = pollFromCreateResponse(tbl, 40); err == nil {
		t.Error("a response without a poll must be an error")
	}

	// A task the server failed is reported with the server's reason, even if the response has a poll.
	tbl.LSAddPollForThread = []*table.LSAddPollForThread{{PollID: 903, ThreadKey: 40, LastUpdateMessageID: "mid.other", LastUpdateMessageEventType: table.PollEventQuestionCreation}}
	tbl.LSHandleFailedTask = []*table.LSHandleFailedTask{{Message: "nope"}}
	if _, _, err = pollFromCreateResponse(tbl, 40); !errors.Is(err, ErrServerRejectedMessage) || !strings.Contains(err.Error(), "nope") {
		t.Errorf("failed task error = %v", err)
	}
}

func TestPollCapabilities(t *testing.T) {
	for name, caps := range map[string]*event.RoomFeatures{"dm": metaCapsPolls, "group": metaCapsGroupPolls, "community": metaCapsWithThreadsPolls} {
		if caps.Poll != event.CapLevelFullySupported || caps.PollEnd != event.CapLevelRejected {
			t.Errorf("%s: poll=%v poll_end=%v", name, caps.Poll, caps.PollEnd)
		}
	}
	for name, caps := range map[string]*event.RoomFeatures{"e2ee": metaCapsWithE2E, "e2ee group": metaCapsWithE2EGroup, "plain": metaCaps} {
		if caps.Poll.Partial() {
			t.Errorf("%s rooms must not claim poll support", name)
		}
	}
	if metaCapsPolls.ID == metaCaps.ID {
		t.Error("rooms with different features need different capability IDs")
	}
}
