package table

import (
	"sort"
	"strconv"
	"strings"
)

// Values of the web client's LSGroupPollEventType, which says what the last update of a poll was.
const (
	PollEventQuestionCreation        int64 = 1
	PollEventAddOption               int64 = 2
	PollEventAddUnvotedOption        int64 = 3
	PollEventUpdateVote              int64 = 4
	PollEventMultipleUpdates         int64 = 5
	PollEventQuestionClosed          int64 = 6
	PollEventQuestionBumped          int64 = 7
	PollEventQuestionDeletionSuccess int64 = 8
	PollEventQuestionDeletionFailure int64 = 9
)

func (ls *LSAddPollForThread) GetThreadKey() int64 {
	return ls.ThreadKey
}

// XMAPollCTAPrefix starts the type of the call to action of the card that shows a poll in a message.
const XMAPollCTAPrefix = "xma_poll_"

// IsPoll says whether the attachment is the card that shows a poll.
func (x *WrappedXMA) IsPoll() bool {
	if x.CTA != nil && strings.HasPrefix(x.CTA.Type_, XMAPollCTAPrefix) {
		return true
	}
	return x.LSInsertXmaAttachment != nil && strings.HasPrefix(x.Cta1Type, XMAPollCTAPrefix)
}

// PollID is the ID of the poll that the card shows. The web client fills the list ID, the ID of the
// call to action's target and the attachment ID of a poll card with the poll's ID; the first that
// is set is used.
func (x *WrappedXMA) PollID() int64 {
	if x.LSInsertXmaAttachment != nil && x.ListItemsId != 0 {
		return x.ListItemsId
	}
	if x.CTA != nil && x.CTA.TargetId != 0 {
		return x.CTA.TargetId
	}
	if x.LSInsertXmaAttachment != nil {
		if id, err := strconv.ParseInt(x.AttachmentFbid, 10, 64); err == nil {
			return id
		}
	}
	return 0
}

// PollQuestion is the question of the poll that the card shows.
func (x *WrappedXMA) PollQuestion() string {
	return x.ListItemsDescriptionText
}

// XMAPollItem is one of the options a poll card lists.
type XMAPollItem struct {
	OptionID  int64
	Text      string
	VoteCount int64
}

// PollItems are the options listed on the poll card. The card only lists the first three, and says
// how many more there are in the secondary description, see PollItemsTruncated.
func (x *WrappedXMA) PollItems() []XMAPollItem {
	all := []XMAPollItem{
		{x.ListItemId1, x.ListItemTitleText1, x.ListItemTotalCount1},
		{x.ListItemId2, x.ListItemTitleText2, x.ListItemTotalCount2},
		{x.ListItemId3, x.ListItemTitleText3, x.ListItemTotalCount3},
	}
	var items []XMAPollItem
	for _, item := range all {
		if item.OptionID != 0 && item.Text != "" {
			items = append(items, item)
		}
	}
	return items
}

// PollItemsTruncated says whether the poll has options the card doesn't list.
func (x *WrappedXMA) PollItemsTruncated() bool {
	return x.ListItemsSecondaryDescriptionText != ""
}

// WrappedPoll is what a table says about one poll.
type WrappedPoll struct {
	PollID    int64
	ThreadKey int64
	Question  string
	// Options are the options the table has rows for, in the order Messenger shows them.
	Options []*LSAddPollOption
	// CardOptions are the options the poll's card lists. It is only used when Options is empty.
	CardOptions []XMAPollItem
	// CardTruncated is set when the card leaves options out.
	CardTruncated bool
	Votes         []*LSAddPollVote
	// LastEventType is the type of the poll's last update, and LastEventMessageID the message that
	// update is about, if the table has a row for the poll.
	LastEventType      int64
	LastEventMessageID string
}

// SortPollOptions puts options in the order Messenger shows them.
func SortPollOptions(options []*LSAddPollOption) {
	sort.SliceStable(options, func(i, j int) bool {
		if options[i].SortKeyCreationTimestamp != options[j].SortKeyCreationTimestamp {
			return options[i].SortKeyCreationTimestamp < options[j].SortKeyCreationTimestamp
		}
		return options[i].OptionID < options[j].OptionID
	})
}

// Closed says whether the poll's last update is that it was closed.
func (wp *WrappedPoll) Closed() bool {
	return wp.LastEventType == PollEventQuestionClosed
}

// WrapPolls groups the poll rows of a table by poll. Rows for the same option or vote that come twice, which
// happens when a table carries both the v1 and the v2 procedure, are merged into one.
func (table *LSTable) WrapPolls() map[int64]*WrappedPoll {
	polls := make(map[int64]*WrappedPoll)
	get := func(pollID int64) *WrappedPoll {
		poll, ok := polls[pollID]
		if !ok {
			poll = &WrappedPoll{PollID: pollID}
			polls[pollID] = poll
		}
		return poll
	}
	for _, row := range table.LSAddPollForThread {
		poll := get(row.PollID)
		poll.ThreadKey = row.ThreadKey
		poll.LastEventType = row.LastUpdateMessageEventType
		poll.LastEventMessageID = row.LastUpdateMessageID
	}
	seenOptions := make(map[[2]int64]bool)
	for _, rows := range [][]*LSAddPollOption{table.LSAddPollOption, table.LSAddPollOptionV2} {
		for _, row := range rows {
			key := [2]int64{row.PollID, row.OptionID}
			if seenOptions[key] {
				continue
			}
			seenOptions[key] = true
			poll := get(row.PollID)
			poll.Options = append(poll.Options, row)
		}
	}
	votes := make(map[[3]int64]*LSAddPollVote)
	for _, rows := range [][]*LSAddPollVote{table.LSAddPollVote, table.LSAddPollVoteV2} {
		for _, row := range rows {
			poll := get(row.PollID)
			key := [3]int64{row.PollID, row.OptionID, row.ContactID}
			if prev, ok := votes[key]; ok {
				if row.TimestampMS > prev.TimestampMS {
					*prev = *row
				}
				continue
			}
			votes[key] = row
			poll.Votes = append(poll.Votes, row)
			if poll.ThreadKey == 0 {
				poll.ThreadKey = row.ThreadKey
			}
		}
	}
	for _, poll := range polls {
		SortPollOptions(poll.Options)
	}
	return polls
}

// IsCreatedBy says whether the message can be the one that started the poll. The row of a poll
// names the message of its last update, so a poll that was just created is known to have been
// started by that message, and a poll that was just voted on is known not to have been started by
// the message about the vote. Anything else could be either.
func (wp *WrappedPoll) IsCreatedBy(messageID string) bool {
	switch {
	case wp.LastEventMessageID == "":
		return true
	case wp.LastEventType == PollEventQuestionCreation:
		return wp.LastEventMessageID == messageID
	default:
		return wp.LastEventMessageID != messageID
	}
}
