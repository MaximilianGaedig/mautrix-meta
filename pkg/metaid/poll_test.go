package metaid

import (
	"encoding/json"
	"reflect"
	"testing"
)

func fbPoll(optionIDs ...int64) *PollMetadata {
	pm := &PollMetadata{PollID: 900}
	for _, id := range optionIDs {
		pm.LearnOption(id, "option "+FBAnswerID(id))
	}
	pm.Frozen = true
	return pm
}

func TestFBAnswerIDsFollowOptionIDs(t *testing.T) {
	pm := fbPoll(11, 12)
	if got, ok := pm.AnswerIDForOption(12); !ok || got != "12" {
		t.Errorf("answer for option 12 = %q, %v", got, ok)
	}
	if _, ok := pm.AnswerIDForOption(99); ok {
		t.Error("an unknown option must not have an answer")
	}
	if _, ok := pm.AnswerIDForOption(0); ok {
		t.Error("option 0 is 'not known yet' and must not match")
	}
}

func TestApplyVoteChangedVoteReplacesEarlierOne(t *testing.T) {
	pm := fbPoll(11, 12, 13)
	answers, changed := pm.ApplyVote(5, []VoteRow{{OptionID: 11, TimestampMS: 1000}})
	if !changed || !reflect.DeepEqual(answers, []string{"11"}) {
		t.Fatalf("first vote = %v, %v", answers, changed)
	}
	answers, changed = pm.ApplyVote(5, []VoteRow{{OptionID: 13, TimestampMS: 2000}})
	if !changed || !reflect.DeepEqual(answers, []string{"13"}) {
		t.Fatalf("changed vote = %v, %v: the new vote must replace the old one, not add to it", answers, changed)
	}
}

func TestApplyVoteRowsOfOneMomentAreOneVote(t *testing.T) {
	pm := fbPoll(11, 12, 13)
	answers, changed := pm.ApplyVote(5, []VoteRow{
		{OptionID: 13, TimestampMS: 3000},
		{OptionID: 11, TimestampMS: 3000},
	})
	if !changed || !reflect.DeepEqual(answers, []string{"11", "13"}) {
		t.Fatalf("multiple choice vote = %v, %v", answers, changed)
	}
}

func TestApplyVoteIgnoresStaleRowsAndRepeats(t *testing.T) {
	pm := fbPoll(11, 12)
	pm.ApplyVote(5, []VoteRow{{OptionID: 12, TimestampMS: 2000}})
	if answers, changed := pm.ApplyVote(5, []VoteRow{{OptionID: 11, TimestampMS: 1000}}); changed {
		t.Errorf("a row older than the vote changed it: %v", answers)
	}
	if answers, changed := pm.ApplyVote(5, []VoteRow{{OptionID: 12, TimestampMS: 2000}}); changed {
		t.Errorf("the same vote a second time changed it: %v", answers)
	}
	if answers, changed := pm.ApplyVote(5, nil); changed || answers != nil {
		t.Errorf("no rows changed the vote: %v", answers)
	}
}

func TestApplyVoteEchoOfOwnVoteIsNotNew(t *testing.T) {
	pm := fbPoll(11, 12)
	pm.SetOwnVote(7, []int64{12, 11})
	if answers, changed := pm.ApplyVote(7, []VoteRow{{OptionID: 11, TimestampMS: 5000}, {OptionID: 12, TimestampMS: 5000}}); changed {
		t.Errorf("the echo of a vote sent from Matrix was bridged again: %v", answers)
	}
	// A different vote made later on Messenger itself still goes through.
	answers, changed := pm.ApplyVote(7, []VoteRow{{OptionID: 12, TimestampMS: 6000}})
	if !changed || !reflect.DeepEqual(answers, []string{"12"}) {
		t.Errorf("later vote = %v, %v", answers, changed)
	}
}

func TestApplyVoteLeavesUnknownAndLateOptionsOutOfAnswers(t *testing.T) {
	pm := fbPoll(11)
	// Option 14 was added after the poll went to Matrix, which can't show it.
	if !pm.LearnOption(14, "added later") {
		t.Fatal("a new option must be learned")
	}
	answers, changed := pm.ApplyVote(5, []VoteRow{{OptionID: 14, TimestampMS: 1000}, {OptionID: 11, TimestampMS: 1000}, {OptionID: 77, TimestampMS: 1000}})
	if !changed || !reflect.DeepEqual(answers, []string{"11"}) {
		t.Errorf("answers = %v, %v: only options the Matrix poll has may be in a response", answers, changed)
	}
}

func TestLearnOptionMatchesMatrixOptionsByText(t *testing.T) {
	pm := &PollMetadata{StartedOnMatrix: true, Frozen: true, Options: []PollOption{
		{AnswerID: "a-tea", Text: "Tea"},
		{AnswerID: "a-coffee", Text: "Coffee"},
	}}
	if !pm.LearnOption(501, "Coffee") || !pm.LearnOption(500, "Tea") {
		t.Fatal("options must be learned")
	}
	if pm.LearnOption(500, "Tea") {
		t.Error("a known option must not change anything")
	}
	if got, ok := pm.AnswerIDForOption(501); !ok || got != "a-coffee" {
		t.Errorf("answer for 501 = %q, %v; the Matrix answer ID must be kept", got, ok)
	}
	if len(pm.Options) != 2 {
		t.Errorf("options = %+v, matching by text must not add options", pm.Options)
	}
}

func TestPollMetadataSurvivesTheDatabase(t *testing.T) {
	pm := fbPoll(11, 12)
	pm.ApplyVote(5, []VoteRow{{OptionID: 12, TimestampMS: 42}})
	raw, err := json.Marshal(&MessageMetadata{Poll: pm})
	if err != nil {
		t.Fatal(err)
	}
	var back MessageMetadata
	if err = json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back.Poll, pm) {
		t.Errorf("round trip changed the metadata:\n got %+v\nwant %+v", back.Poll, pm)
	}
}
