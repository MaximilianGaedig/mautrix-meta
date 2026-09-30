package msgconv

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

const (
	pollKindDisclosed = "org.matrix.msc3381.poll.disclosed"

	// The fewest options a poll needs. Messenger's own limits for the number and length of options
	// are not known, so the server is left to reject a poll that is too big.
	minPollOptions = 2
)

// PollStartContent is the content of an org.matrix.msc3381.poll.start event: the plain text
// fallback, and in the extra fields the poll itself, which any Matrix client that knows polls
// shows instead of the fallback. Messenger polls allow choosing any number of options.
func PollStartContent(question string, options []metaid.PollOption, closed bool) (*event.MessageEventContent, map[string]any) {
	answers := make([]map[string]any, len(options))
	textAnswers := make([]string, len(options))
	var htmlAnswers strings.Builder
	for i, opt := range options {
		answers[i] = map[string]any{
			"id":                      opt.AnswerID,
			"org.matrix.msc1767.text": opt.Text,
		}
		textAnswers[i] = fmt.Sprintf("%d. %s", i+1, opt.Text)
		htmlAnswers.WriteString("<li>" + event.TextToHTML(opt.Text) + "</li>")
	}
	body := fmt.Sprintf("Poll: %s\n\n%s", question, strings.Join(textAnswers, "\n"))
	formattedBody := fmt.Sprintf("<p><strong>Poll</strong>: %s</p><ol>%s</ol>", event.TextToHTML(question), htmlAnswers.String())
	if closed {
		body += "\n\n(This poll is closed.)"
		formattedBody += "<p>(This poll is closed.)</p>"
	}
	content := &event.MessageEventContent{
		MsgType:       event.MsgText,
		Body:          body,
		Format:        event.FormatHTML,
		FormattedBody: formattedBody,
	}
	extra := map[string]any{
		"org.matrix.msc1767.message": []map[string]any{
			{"mimetype": "text/html", "body": formattedBody},
			{"mimetype": "text/plain", "body": body},
		},
		"org.matrix.msc3381.poll.start": map[string]any{
			"kind":           pollKindDisclosed,
			"max_selections": max(len(options), 1),
			"question": map[string]any{
				"org.matrix.msc1767.text": question,
			},
			"answers": answers,
		},
	}
	return content, extra
}

// PollFromTable converts a poll of a Messenger table to a poll start part, and the metadata that
// has to be saved with it to bridge votes. It returns false if the table doesn't know enough about
// the poll to show it: the question or the options are missing, or the poll's card leaves some
// options out and no rows list them.
func PollFromTable(poll *table.WrappedPoll) (*bridgev2.ConvertedMessagePart, *metaid.PollMetadata, bool) {
	if poll == nil || poll.PollID == 0 || strings.TrimSpace(poll.Question) == "" {
		return nil, nil, false
	}
	meta := &metaid.PollMetadata{PollID: poll.PollID}
	for _, opt := range poll.Options {
		if opt.OptionText == "" {
			continue
		}
		meta.LearnOption(opt.OptionID, opt.OptionText)
	}
	if len(meta.Options) == 0 && !poll.CardTruncated {
		for _, item := range poll.CardOptions {
			meta.LearnOption(item.OptionID, item.Text)
		}
	}
	if len(meta.Options) == 0 {
		return nil, nil, false
	}
	meta.Closed = poll.Closed()
	meta.Frozen = true
	content, extra := PollStartContent(poll.Question, meta.Options, meta.Closed)
	return &bridgev2.ConvertedMessagePart{
		Type:       event.EventUnstablePollStart,
		Content:    content,
		Extra:      extra,
		DBMetadata: &metaid.MessageMetadata{Poll: meta},
	}, meta, true
}

// PollFallbackText describes a poll that can't be bridged as a poll, from what its card shows.
func PollFallbackText(poll *table.WrappedPoll) string {
	var lines []string
	if q := strings.TrimSpace(poll.Question); q != "" {
		lines = append(lines, "Poll: "+q)
	} else {
		lines = append(lines, "Poll")
	}
	options := poll.CardOptions
	for i, item := range options {
		lines = append(lines, fmt.Sprintf("%d. %s", i+1, item.Text))
	}
	if poll.CardTruncated {
		lines = append(lines, "(and more options, open Messenger to see them)")
	}
	return strings.Join(lines, "\n")
}

// PollResponseContent is the content of an org.matrix.msc3381.poll.response event. An empty list
// retracts the vote.
func PollResponseContent(pollEventID id.EventID, answerIDs []string) (*event.MessageEventContent, map[string]any) {
	if answerIDs == nil {
		answerIDs = []string{}
	}
	content := &event.MessageEventContent{
		RelatesTo: &event.RelatesTo{Type: event.RelReference, EventID: pollEventID},
	}
	extra := map[string]any{
		"org.matrix.msc3381.poll.response": map[string]any{"answers": answerIDs},
	}
	return content, extra
}

// PollEndContent is the content of an org.matrix.msc3381.poll.end event.
func PollEndContent(pollEventID id.EventID) (*event.MessageEventContent, map[string]any) {
	const text = "The poll has ended."
	content := &event.MessageEventContent{
		MsgType:   event.MsgText,
		Body:      text,
		RelatesTo: &event.RelatesTo{Type: event.RelReference, EventID: pollEventID},
	}
	extra := map[string]any{
		"org.matrix.msc3381.poll.end": map[string]any{},
		"org.matrix.msc1767.text":     text,
	}
	return content, extra
}

// MatrixPollToTask is the task that creates a poll started on Matrix, and the metadata to save
// with the poll's message. The options don't have Messenger IDs yet: Messenger makes them up when
// it creates the poll.
func MatrixPollToTask(threadKey int64, content *event.PollStartEventContent) (*socket.CreatePollTask, *metaid.PollMetadata, error) {
	start := &content.PollStart
	question := strings.TrimSpace(start.Question.GetText())
	if question == "" {
		return nil, nil, errors.New("the poll has no question")
	} else if len(start.Answers) < minPollOptions {
		return nil, nil, fmt.Errorf("polls on Messenger need at least %d options", minPollOptions)
	}
	meta := &metaid.PollMetadata{StartedOnMatrix: true, Frozen: true}
	texts := make([]string, len(start.Answers))
	for i, answer := range start.Answers {
		text := strings.TrimSpace(answer.GetText())
		if text == "" {
			return nil, nil, fmt.Errorf("option %d of the poll is empty", i+1)
		} else if slices.Contains(texts[:i], text) {
			// Options are matched with Messenger's by their text.
			return nil, nil, fmt.Errorf("the poll has the option %q twice", text)
		} else if _, exists := meta.OptionForAnswer(answer.ID); exists {
			return nil, nil, fmt.Errorf("the poll has two options with the ID %q", answer.ID)
		}
		texts[i] = text
		meta.Options = append(meta.Options, metaid.PollOption{AnswerID: answer.ID, Text: text})
	}
	return &socket.CreatePollTask{
		QuestionText: question,
		ThreadKey:    threadKey,
		Options:      texts,
		SyncGroup:    1,
	}, meta, nil
}

// MatrixVoteToTask is the task that sets the vote of the logged in user on a poll to the options a
// Matrix poll response picks. A response without options takes the vote back. It also returns the
// Messenger IDs of the options, to remember the vote.
func MatrixVoteToTask(threadKey int64, meta *metaid.PollMetadata, answers []string) (*socket.UpdatePollTask, []int64, error) {
	if meta == nil || meta.PollID == 0 {
		return nil, nil, errors.New("the poll has no Messenger ID, so it can't be voted on")
	}
	selected := make([]int64, 0, len(answers))
	for _, answerID := range answers {
		opt, ok := meta.OptionForAnswer(answerID)
		if !ok {
			return nil, nil, fmt.Errorf("unknown poll answer %q", answerID)
		} else if opt.OptionID == 0 {
			return nil, nil, fmt.Errorf("no ID from Messenger yet for the option %q, try again in a moment", opt.Text)
		}
		if !slices.Contains(selected, opt.OptionID) {
			selected = append(selected, opt.OptionID)
		}
	}
	return &socket.UpdatePollTask{
		ThreadKey:       threadKey,
		PollID:          meta.PollID,
		AddedOptions:    []map[string]int{},
		SelectedOptions: selected,
		SyncGroup:       1,
	}, selected, nil
}
