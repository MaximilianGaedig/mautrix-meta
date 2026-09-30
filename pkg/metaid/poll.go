package metaid

import (
	"slices"
	"strconv"
)

// PollOption ties a Matrix poll answer to a Messenger poll option.
type PollOption struct {
	// AnswerID is the answer ID in the Matrix poll. For polls that started on Messenger it is the
	// option ID in decimal, so it never needs to be looked up; for polls that started on Matrix it is
	// whatever the Matrix client chose.
	AnswerID string `json:"answer_id"`
	// OptionID is Messenger's ID of the option. It is 0 for an option of a poll started on Matrix
	// until Messenger has told us which ID it gave the option.
	OptionID int64  `json:"option_id,omitempty"`
	Text     string `json:"text"`
	// Late is set for options that were added after the poll was sent to Matrix, which Matrix
	// polls can't show, so they can't be voted for there.
	Late bool `json:"late,omitempty"`
}

// PollVote is what one person currently has voted for.
type PollVote struct {
	// TimestampMS is when the vote was made. All options of one vote share it.
	TimestampMS int64   `json:"ts"`
	Options     []int64 `json:"options"`
}

// PollMetadata is saved with the message that carries a poll.
type PollMetadata struct {
	PollID  int64        `json:"poll_id"`
	Options []PollOption `json:"options"`
	// Votes holds the last vote of each voter that was bridged, by Messenger contact ID.
	Votes map[int64]*PollVote `json:"votes,omitempty"`
	// StartedOnMatrix is set for polls that were created from Matrix.
	StartedOnMatrix bool `json:"started_on_matrix,omitempty"`
	Closed          bool `json:"closed,omitempty"`
	// Frozen is set once the poll was sent to Matrix or Messenger, from which on new options are late.
	Frozen bool `json:"frozen,omitempty"`
}

// FBAnswerID is the Matrix answer ID of a Messenger option of a poll that started on Messenger.
func FBAnswerID(optionID int64) string {
	return strconv.FormatInt(optionID, 10)
}

func (pm *PollMetadata) AnswerIDForOption(optionID int64) (string, bool) {
	for _, opt := range pm.Options {
		if opt.OptionID == optionID && optionID != 0 && !opt.Late {
			return opt.AnswerID, true
		}
	}
	return "", false
}

func (pm *PollMetadata) OptionForAnswer(answerID string) (*PollOption, bool) {
	for i := range pm.Options {
		if pm.Options[i].AnswerID == answerID {
			return &pm.Options[i], true
		}
	}
	return nil, false
}

// LearnOption records Messenger's ID for an option. An option that is already known by ID is left
// alone; a new ID is matched to an option of a Matrix-started poll that has no ID yet by its text,
// and otherwise added as a new option. It reports whether anything changed.
func (pm *PollMetadata) LearnOption(optionID int64, text string) bool {
	if optionID == 0 {
		return false
	}
	for _, opt := range pm.Options {
		if opt.OptionID == optionID {
			return false
		}
	}
	for i := range pm.Options {
		if pm.Options[i].OptionID == 0 && pm.Options[i].Text == text {
			pm.Options[i].OptionID = optionID
			return true
		}
	}
	pm.Options = append(pm.Options, PollOption{AnswerID: FBAnswerID(optionID), OptionID: optionID, Text: text, Late: pm.Frozen})
	return true
}

// VoteRow is one option that Messenger says somebody voted for.
type VoteRow struct {
	OptionID    int64
	TimestampMS int64
}

// ApplyVote merges the rows Messenger sent for one voter into what is known about their vote.
//
// A vote is the whole set of options somebody chose at one moment, and Messenger sends one row per
// option, all with the moment's timestamp. So the newest timestamp defines the vote: rows with that
// timestamp are added to it, rows with a newer one replace it, and older rows are stale. It returns
// the Matrix answer IDs of the vote and whether the vote is different from before. Options that
// are not known are left out of the answers, but still count as part of the vote.
func (pm *PollMetadata) ApplyVote(contactID int64, rows []VoteRow) (answerIDs []string, changed bool) {
	if len(rows) == 0 {
		return nil, false
	}
	if pm.Votes == nil {
		pm.Votes = make(map[int64]*PollVote)
	}
	old := pm.Votes[contactID]
	cur := &PollVote{}
	if old != nil {
		cur = &PollVote{TimestampMS: old.TimestampMS, Options: slices.Clone(old.Options)}
	}
	for _, row := range rows {
		switch {
		case row.TimestampMS > cur.TimestampMS:
			cur = &PollVote{TimestampMS: row.TimestampMS, Options: []int64{row.OptionID}}
		case row.TimestampMS == cur.TimestampMS && !slices.Contains(cur.Options, row.OptionID):
			cur.Options = append(cur.Options, row.OptionID)
		}
	}
	slices.Sort(cur.Options)
	pm.Votes[contactID] = cur
	if old != nil && slices.Equal(old.Options, cur.Options) {
		// Same options at a later time, which is what the echo of our own vote looks like.
		return nil, false
	}
	for _, optionID := range cur.Options {
		if answerID, ok := pm.AnswerIDForOption(optionID); ok {
			answerIDs = append(answerIDs, answerID)
		}
	}
	return answerIDs, true
}

// SetOwnVote records a vote that was sent from Matrix, so the copy Messenger sends back is not
// bridged a second time. The vote gets no timestamp of its own: our clock is not Messenger's, and a
// timestamp from the future would make every later vote look stale.
func (pm *PollMetadata) SetOwnVote(contactID int64, optionIDs []int64) {
	if pm.Votes == nil {
		pm.Votes = make(map[int64]*PollVote)
	}
	options := slices.Clone(optionIDs)
	slices.Sort(options)
	pm.Votes[contactID] = &PollVote{Options: options}
}
