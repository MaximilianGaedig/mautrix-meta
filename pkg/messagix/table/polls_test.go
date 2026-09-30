package table

import (
	"reflect"
	"testing"
)

func TestWrapPollsGroupsRowsByPoll(t *testing.T) {
	tbl := &LSTable{
		LSAddPollForThread: []*LSAddPollForThread{
			{PollID: 900, ThreadKey: 40, LastUpdateMessageID: "mid.creation", LastUpdateMessageEventType: PollEventQuestionCreation},
		},
		LSAddPollOption: []*LSAddPollOption{
			{OptionID: 12, PollID: 900, OptionText: "Second", SortKeyCreationTimestamp: 2},
			{OptionID: 11, PollID: 900, OptionText: "First", SortKeyCreationTimestamp: 1},
			{OptionID: 21, PollID: 901, OptionText: "Other poll"},
		},
		// The same option again, the way a v2 procedure repeats a v1 one.
		LSAddPollOptionV2: []*LSAddPollOption{{OptionID: 11, PollID: 900, OptionText: "First", SortKeyCreationTimestamp: 1}},
		LSAddPollVote: []*LSAddPollVote{
			{OptionID: 11, PollID: 900, ContactID: 5, TimestampMS: 100},
		},
		LSAddPollVoteV2: []*LSAddPollVote{
			{OptionID: 11, PollID: 900, ContactID: 5, TimestampMS: 200, ThreadKey: 40},
			{OptionID: 12, PollID: 900, ContactID: 6, TimestampMS: 150, ThreadKey: 40},
		},
	}
	polls := tbl.WrapPolls()
	if len(polls) != 2 {
		t.Fatalf("got %d polls, want 2", len(polls))
	}
	p := polls[900]
	if p.ThreadKey != 40 || p.LastEventType != PollEventQuestionCreation || p.LastEventMessageID != "mid.creation" {
		t.Errorf("poll row was not read: %+v", p)
	}
	var texts []string
	for _, opt := range p.Options {
		texts = append(texts, opt.OptionText)
	}
	if !reflect.DeepEqual(texts, []string{"First", "Second"}) {
		t.Errorf("options = %v, want each once in Messenger's order", texts)
	}
	if len(p.Votes) != 2 {
		t.Fatalf("votes = %d, want 2 (a repeated vote is one vote)", len(p.Votes))
	}
	for _, v := range p.Votes {
		if v.ContactID == 5 && v.TimestampMS != 200 {
			t.Errorf("repeated vote kept timestamp %d, want the newer 200", v.TimestampMS)
		}
	}
	if q := polls[901]; q == nil || len(q.Options) != 1 || q.ThreadKey != 0 {
		t.Errorf("second poll = %+v", q)
	}
}

func TestVotesGiveThePollsThreadWhenNoRowDoes(t *testing.T) {
	polls := (&LSTable{LSAddPollVoteV2: []*LSAddPollVote{{OptionID: 1, PollID: 7, ContactID: 5, ThreadKey: 40}}}).WrapPolls()
	if polls[7].ThreadKey != 40 {
		t.Errorf("thread key = %d", polls[7].ThreadKey)
	}
}

func pollCardMessage(messageID string, pollID int64) (*LSInsertMessage, *LSInsertXmaAttachment, *LSInsertAttachmentCta) {
	return &LSInsertMessage{MessageId: messageID, ThreadKey: 40},
		&LSInsertXmaAttachment{
			MessageId:                         messageID,
			AttachmentFbid:                    "123456",
			ThreadKey:                         40,
			ListItemsId:                       pollID,
			ListItemsDescriptionText:          "Where do we eat?",
			ListItemsSecondaryDescriptionText: "2 more options",
			ListItemId1:                       11,
			ListItemTitleText1:                "Pizza",
			ListItemTotalCount1:               3,
			ListItemId2:                       12,
			ListItemTitleText2:                "Sushi",
			ListItemId3:                       13,
			ListItemTitleText3:                "Tacos",
		},
		&LSInsertAttachmentCta{AttachmentFbid: "123456", Type_: "xma_poll_details_card", TargetId: pollID}
}

func TestWrapMessagesAttachesThePollOfAPollCard(t *testing.T) {
	msg, xma, cta := pollCardMessage("mid.creation", 900)
	tbl := &LSTable{
		LSInsertMessage:       []*LSInsertMessage{msg},
		LSInsertXmaAttachment: []*LSInsertXmaAttachment{xma},
		LSInsertAttachmentCta: []*LSInsertAttachmentCta{cta},
		LSAddPollOption:       []*LSAddPollOption{{OptionID: 11, PollID: 900, OptionText: "Pizza"}},
	}
	_, insert := tbl.WrapMessages()
	if len(insert) != 1 || insert[0].Poll == nil {
		t.Fatalf("the poll card's message has no poll: %+v", insert)
	}
	poll := insert[0].Poll
	if poll.PollID != 900 || poll.Question != "Where do we eat?" || len(poll.Options) != 1 || len(poll.CardOptions) != 3 || !poll.CardTruncated {
		t.Errorf("poll = %+v", poll)
	}
	if !insert[0].XMAAttachments[0].IsPoll() {
		t.Error("the card must be recognised as a poll")
	}
}

func TestPollCardWithoutPollRowsStillGetsAPoll(t *testing.T) {
	msg, xma, cta := pollCardMessage("mid.creation", 900)
	tbl := &LSTable{
		LSInsertMessage:       []*LSInsertMessage{msg},
		LSInsertXmaAttachment: []*LSInsertXmaAttachment{xma},
		LSInsertAttachmentCta: []*LSInsertAttachmentCta{cta},
	}
	_, insert := tbl.WrapMessages()
	if insert[0].Poll == nil || insert[0].Poll.PollID != 900 || insert[0].Poll.ThreadKey != 40 {
		t.Errorf("poll = %+v", insert[0].Poll)
	}
}

func TestPollIDFallsBackToTheCTATargetAndAttachmentID(t *testing.T) {
	x := &WrappedXMA{LSInsertXmaAttachment: &LSInsertXmaAttachment{AttachmentFbid: "77"}, CTA: &LSInsertAttachmentCta{Type_: "xma_poll_details_card", TargetId: 55}}
	if x.PollID() != 55 {
		t.Errorf("poll ID = %d, want the CTA target 55", x.PollID())
	}
	x.CTA.TargetId = 0
	if x.PollID() != 77 {
		t.Errorf("poll ID = %d, want the attachment ID 77", x.PollID())
	}
	other := &WrappedXMA{LSInsertXmaAttachment: &LSInsertXmaAttachment{}, CTA: &LSInsertAttachmentCta{Type_: "xma_web_url"}}
	if other.IsPoll() {
		t.Error("a link card is not a poll")
	}
}

func TestIsCreatedBy(t *testing.T) {
	cases := []struct {
		name string
		poll WrappedPoll
		msg  string
		want bool
	}{
		{"no row", WrappedPoll{}, "mid.a", true},
		{"just created, this message", WrappedPoll{LastEventType: PollEventQuestionCreation, LastEventMessageID: "mid.a"}, "mid.a", true},
		{"just created, other message", WrappedPoll{LastEventType: PollEventQuestionCreation, LastEventMessageID: "mid.a"}, "mid.b", false},
		{"just voted, this is the vote message", WrappedPoll{LastEventType: PollEventUpdateVote, LastEventMessageID: "mid.a"}, "mid.a", false},
		{"just voted, other message", WrappedPoll{LastEventType: PollEventUpdateVote, LastEventMessageID: "mid.a"}, "mid.b", true},
	}
	for _, tc := range cases {
		if got := tc.poll.IsCreatedBy(tc.msg); got != tc.want {
			t.Errorf("%s: IsCreatedBy = %v, want %v", tc.name, got, tc.want)
		}
	}
}
