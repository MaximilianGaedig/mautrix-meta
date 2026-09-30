package msgconv

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

func testPoll() *table.WrappedPoll {
	return &table.WrappedPoll{
		PollID:   900,
		Question: "Where do we eat?",
		Options: []*table.LSAddPollOption{
			{OptionID: 11, PollID: 900, OptionText: "Pizza"},
			{OptionID: 12, PollID: 900, OptionText: "Sushi"},
			{OptionID: 13, PollID: 900, OptionText: "Tacos"},
		},
	}
}

func TestPollFromTableMakesAPollStart(t *testing.T) {
	part, meta, ok := PollFromTable(testPoll())
	if !ok {
		t.Fatal("a poll with a question and options must convert")
	}
	if part.Type != event.EventUnstablePollStart {
		t.Errorf("type = %v", part.Type)
	}
	start, _ := part.Extra["org.matrix.msc3381.poll.start"].(map[string]any)
	if start == nil {
		t.Fatalf("extra has no poll start: %+v", part.Extra)
	}
	if start["max_selections"] != 3 || start["kind"] != "org.matrix.msc3381.poll.disclosed" {
		t.Errorf("start = %+v", start)
	}
	question, _ := start["question"].(map[string]any)
	if question["org.matrix.msc1767.text"] != "Where do we eat?" {
		t.Errorf("question = %+v", question)
	}
	answers, _ := start["answers"].([]map[string]any)
	var ids, texts []any
	for _, a := range answers {
		ids = append(ids, a["id"])
		texts = append(texts, a["org.matrix.msc1767.text"])
	}
	if !reflect.DeepEqual(ids, []any{"11", "12", "13"}) || !reflect.DeepEqual(texts, []any{"Pizza", "Sushi", "Tacos"}) {
		t.Errorf("answers ids=%v texts=%v: answer IDs must be the option IDs", ids, texts)
	}
	if !strings.Contains(part.Content.Body, "Where do we eat?") || !strings.Contains(part.Content.Body, "2. Sushi") {
		t.Errorf("no text fallback: %q", part.Content.Body)
	}
	if got := part.DBMetadata.(*metaid.MessageMetadata).Poll; got != meta || meta.PollID != 900 || len(meta.Options) != 3 || !meta.Frozen {
		t.Errorf("metadata = %+v", meta)
	}
}

func TestPollFromTableClosedPoll(t *testing.T) {
	poll := testPoll()
	poll.LastEventType = table.PollEventQuestionClosed
	part, meta, ok := PollFromTable(poll)
	if !ok || !meta.Closed || !strings.Contains(part.Content.Body, "closed") {
		t.Errorf("closed poll: ok=%v meta=%+v body=%q", ok, meta, part.Content.Body)
	}
}

func TestPollFromTableUsesTheCardOnlyWhenItListsEverything(t *testing.T) {
	poll := &table.WrappedPoll{PollID: 900, Question: "Q?", CardOptions: []table.XMAPollItem{{OptionID: 1, Text: "A"}, {OptionID: 2, Text: "B"}}}
	if _, meta, ok := PollFromTable(poll); !ok || len(meta.Options) != 2 || meta.Options[0].AnswerID != "1" {
		t.Errorf("complete card: ok=%v meta=%+v", ok, meta)
	}
	poll.CardTruncated = true
	if _, _, ok := PollFromTable(poll); ok {
		t.Error("a card that leaves options out must not make a poll with only some of the options")
	}
	if text := PollFallbackText(poll); !strings.Contains(text, "Q?") || !strings.Contains(text, "1. A") || !strings.Contains(text, "more options") {
		t.Errorf("fallback text = %q", text)
	}
}

func TestPollFromTableNeedsAQuestionAndOptions(t *testing.T) {
	noQuestion := testPoll()
	noQuestion.Question = "  "
	noOptions := testPoll()
	noOptions.Options = nil
	for name, poll := range map[string]*table.WrappedPoll{"question": noQuestion, "options": noOptions, "nil": nil} {
		if _, _, ok := PollFromTable(poll); ok {
			t.Errorf("a poll without %s converted", name)
		}
	}
}

func pollMessage() *table.WrappedMessage {
	return &table.WrappedMessage{
		LSInsertMessage: &table.LSInsertMessage{MessageId: "mid.creation", Text: "Anna created a poll", IsAdminMessage: true},
		XMAAttachments: []*table.WrappedXMA{{
			LSInsertXmaAttachment: &table.LSInsertXmaAttachment{ListItemsId: 900},
			CTA:                   &table.LSInsertAttachmentCta{Type_: "xma_poll_details_card"},
		}},
		Poll: testPoll(),
	}
}

func TestToMatrixBridgesPollCardAsPoll(t *testing.T) {
	mc := &MessageConverter{}
	cm := mc.ToMatrix(context.Background(), nil, nil, nil, nil, "fb:mid.creation", pollMessage())
	var poll, text int
	for _, part := range cm.Parts {
		switch {
		case part.Type == event.EventUnstablePollStart:
			poll++
			if part.DontBridge {
				t.Error("the poll must be bridged")
			}
		case part.Content.MsgType == event.MsgNotice:
			text++
			if !part.DontBridge {
				t.Error("the announcement of the poll must not be bridged next to the poll")
			}
		}
	}
	if poll != 1 || text != 1 || len(cm.Parts) != 2 {
		t.Fatalf("parts = %d poll, %d text, of %d", poll, text, len(cm.Parts))
	}
}

func TestToMatrixSkipsARepeatOfABridgedPoll(t *testing.T) {
	msg := pollMessage()
	msg.PollAlreadyBridged = true
	msg.Text = "Ben voted"
	cm := (&MessageConverter{}).ToMatrix(context.Background(), nil, nil, nil, nil, "fb:mid.vote", msg)
	for _, part := range cm.Parts {
		if part.Type == event.EventUnstablePollStart {
			t.Fatal("the same poll was bridged twice")
		}
	}
	if len(cm.Parts) != 1 || cm.Parts[0].Content.Body != "Ben voted" || cm.Parts[0].DontBridge {
		t.Errorf("the service message must still be bridged as text: %+v", cm.Parts)
	}
}

func TestToMatrixFallsBackToTextForAnIncompletePoll(t *testing.T) {
	msg := pollMessage()
	msg.Poll.Options = nil
	msg.Poll.CardOptions = []table.XMAPollItem{{OptionID: 11, Text: "Pizza"}}
	msg.Poll.CardTruncated = true
	cm := (&MessageConverter{}).ToMatrix(context.Background(), nil, nil, nil, nil, "fb:mid.creation", msg)
	found := false
	for _, part := range cm.Parts {
		if part.Type == event.EventUnstablePollStart {
			t.Fatal("made a poll with only some of the options")
		}
		if strings.Contains(part.Content.Body, "Where do we eat?") && strings.Contains(part.Content.Body, "Pizza") {
			found = true
		}
	}
	if !found {
		t.Errorf("no text for the poll: %+v", cm.Parts)
	}
}

func matrixPoll(answers ...event.PollOption) *event.PollStartEventContent {
	return &event.PollStartEventContent{PollStart: event.PollStart{
		Kind:          "org.matrix.msc3381.poll.disclosed",
		MaxSelections: 1,
		Question:      event.MSC1767Message{Text: "  Best colour?  "},
		Answers:       answers,
	}}
}

func answer(id, text string) event.PollOption {
	return event.PollOption{ID: id, MSC1767Message: event.MSC1767Message{Text: text}}
}

func TestMatrixPollToTaskPayload(t *testing.T) {
	task, meta, err := MatrixPollToTask(40, matrixPoll(answer("a1", "Red"), answer("a2", " Blue ")))
	if err != nil {
		t.Fatal(err)
	}
	if task.GetLabel() != "163" {
		t.Errorf("label = %s", task.GetLabel())
	}
	_, queue := task.Create()
	if queue != "poll_creation" {
		t.Errorf("queue = %s", queue)
	}
	raw, _ := json.Marshal(task)
	want := `{"question_text":"Best colour?","thread_key":40,"options":["Red","Blue"],"sync_group":1}`
	if string(raw) != want {
		t.Errorf("payload = %s\n   want = %s", raw, want)
	}
	if !meta.StartedOnMatrix || len(meta.Options) != 2 || meta.Options[1].AnswerID != "a2" || meta.Options[1].Text != "Blue" || meta.Options[1].OptionID != 0 {
		t.Errorf("metadata = %+v: the answer IDs must be kept for the votes that come later", meta)
	}
}

func TestMatrixPollToTaskRejectsPollsMessengerCantHave(t *testing.T) {
	cases := map[string]*event.PollStartEventContent{
		"no question":    {PollStart: event.PollStart{Answers: []event.PollOption{answer("a", "x"), answer("b", "y")}}},
		"one option":     matrixPoll(answer("a", "x")),
		"empty option":   matrixPoll(answer("a", "x"), answer("b", " ")),
		"same text":      matrixPoll(answer("a", "x"), answer("b", "x")),
		"same answer id": matrixPoll(answer("a", "x"), answer("a", "y")),
	}
	for name, content := range cases {
		if _, _, err := MatrixPollToTask(40, content); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestMatrixVoteToTaskPayload(t *testing.T) {
	meta := &metaid.PollMetadata{PollID: 900, Options: []metaid.PollOption{
		{AnswerID: "a1", OptionID: 501, Text: "Red"},
		{AnswerID: "a2", OptionID: 502, Text: "Blue"},
		{AnswerID: "a3", Text: "Green"},
	}}
	task, optionIDs, err := MatrixVoteToTask(40, meta, []string{"a2", "a1", "a2"})
	if err != nil {
		t.Fatal(err)
	}
	if task.GetLabel() != "164" {
		t.Errorf("label = %s", task.GetLabel())
	}
	if _, queue := task.Create(); queue != "poll_update" {
		t.Errorf("queue = %s", queue)
	}
	raw, _ := json.Marshal(task)
	want := `{"thread_key":40,"poll_id":900,"added_options":[],"selected_options":[502,501],"sync_group":1}`
	if string(raw) != want {
		t.Errorf("payload = %s\n   want = %s", raw, want)
	}
	if !reflect.DeepEqual(optionIDs, []int64{502, 501}) {
		t.Errorf("option IDs = %v", optionIDs)
	}

	// Taking the vote back sends an empty list, not null.
	task, _, err = MatrixVoteToTask(40, meta, nil)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(task)
	if !strings.Contains(string(raw), `"selected_options":[]`) {
		t.Errorf("retraction payload = %s", raw)
	}

	if _, _, err = MatrixVoteToTask(40, meta, []string{"nope"}); err == nil {
		t.Error("an unknown answer must be an error")
	}
	if _, _, err = MatrixVoteToTask(40, meta, []string{"a3"}); err == nil {
		t.Error("an option without a Messenger ID must be an error, not vote for option 0")
	}
	if _, _, err = MatrixVoteToTask(40, nil, []string{"a1"}); err == nil {
		t.Error("a poll without metadata must be an error")
	}
}

func TestPollResponseAndEndContent(t *testing.T) {
	content, extra := PollResponseContent("$poll", []string{"11", "13"})
	if content.RelatesTo.Type != event.RelReference || content.RelatesTo.EventID != "$poll" {
		t.Errorf("relation = %+v", content.RelatesTo)
	}
	resp, _ := extra["org.matrix.msc3381.poll.response"].(map[string]any)
	if !reflect.DeepEqual(resp["answers"], []string{"11", "13"}) {
		t.Errorf("response = %+v", extra)
	}
	_, extra = PollResponseContent("$poll", nil)
	resp, _ = extra["org.matrix.msc3381.poll.response"].(map[string]any)
	if !reflect.DeepEqual(resp["answers"], []string{}) {
		t.Errorf("an empty vote must be an empty list, got %#v", resp["answers"])
	}
	content, extra = PollEndContent("$poll")
	if content.RelatesTo.EventID != "$poll" || extra["org.matrix.msc3381.poll.end"] == nil {
		t.Errorf("end = %+v %+v", content, extra)
	}
}
