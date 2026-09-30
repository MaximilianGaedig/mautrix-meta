package connector

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/metaid"
	"go.mau.fi/mautrix-meta/pkg/msgconv"
)

var _ bridgev2.PollHandlingNetworkAPI = (*MetaClient)(nil)

const (
	pollVoteIDPrefix = "poll-vote:"
	pollEndIDPrefix  = "poll-end:"
)

// makePollVoteID is the bridge message ID of a vote. It never parses as a Messenger message ID. The
// time and the options are part of it so that somebody who changes their vote and then changes it
// back gets a new event instead of a duplicate.
func makePollVoteID(pollID, contactID, timestampMS int64, optionIDs []int64) networkid.MessageID {
	options := make([]string, len(optionIDs))
	for i, id := range optionIDs {
		options[i] = strconv.FormatInt(id, 10)
	}
	return networkid.MessageID(fmt.Sprintf("%s%d:%d:%d:%s", pollVoteIDPrefix, pollID, contactID, timestampMS, strings.Join(options, ",")))
}

func parsePollVoteID(messageID networkid.MessageID) (pollID, contactID int64, ok bool) {
	rest, ok := strings.CutPrefix(string(messageID), pollVoteIDPrefix)
	if !ok {
		return 0, 0, false
	}
	parts := strings.SplitN(rest, ":", 4)
	if len(parts) != 4 {
		return 0, 0, false
	}
	pollID, err1 := strconv.ParseInt(parts[0], 10, 64)
	contactID, err2 := strconv.ParseInt(parts[1], 10, 64)
	return pollID, contactID, err1 == nil && err2 == nil
}

// pollRow is what a table says about one voter of a poll, or about the poll being closed.
type pollRow struct {
	ThreadKey int64
	Poll      *table.WrappedPoll
	// ContactID and Votes are the voter and their options. They are empty for a closed poll.
	ContactID int64
	Votes     []metaid.VoteRow
	Closed    bool
}

func (r *pollRow) GetThreadKey() int64 {
	return r.ThreadKey
}

func (r *pollRow) maxTimestampMS() int64 {
	var ts int64
	for _, vote := range r.Votes {
		ts = max(ts, vote.TimestampMS)
	}
	return ts
}

func (r *pollRow) optionIDs() []int64 {
	ids := make([]int64, len(r.Votes))
	for i, vote := range r.Votes {
		ids[i] = vote.OptionID
	}
	slices.Sort(ids)
	return ids
}

// pollRows splits the polls of a table into what happens to each: one row for every voter, and one
// for a poll that was closed. A row of a poll whose thread the table doesn't say is asked from
// threadOf, and dropped if that doesn't know either.
func pollRows(polls map[int64]*table.WrappedPoll, threadOf func(pollID int64) int64) []*pollRow {
	pollIDs := make([]int64, 0, len(polls))
	for pollID := range polls {
		pollIDs = append(pollIDs, pollID)
	}
	slices.Sort(pollIDs)
	var rows []*pollRow
	for _, pollID := range pollIDs {
		poll := polls[pollID]
		threadKey := poll.ThreadKey
		if threadKey == 0 && threadOf != nil {
			threadKey = threadOf(pollID)
		}
		if threadKey == 0 {
			continue
		}
		byContact := make(map[int64]*pollRow)
		var contacts []int64
		for _, vote := range poll.Votes {
			row, ok := byContact[vote.ContactID]
			if !ok {
				row = &pollRow{ThreadKey: threadKey, Poll: poll, ContactID: vote.ContactID}
				byContact[vote.ContactID] = row
				contacts = append(contacts, vote.ContactID)
			}
			row.Votes = append(row.Votes, metaid.VoteRow{OptionID: vote.OptionID, TimestampMS: vote.TimestampMS})
		}
		slices.Sort(contacts)
		for _, contactID := range contacts {
			rows = append(rows, byContact[contactID])
		}
		if poll.Closed() {
			rows = append(rows, &pollRow{ThreadKey: threadKey, Poll: poll, Closed: true})
		}
	}
	return rows
}

func (m *MetaClient) pollRows(ctx context.Context, tbl *table.LSTable) []*pollRow {
	polls := tbl.WrapPolls()
	if len(polls) == 0 {
		return nil
	}
	return pollRows(polls, func(pollID int64) int64 {
		threadKey, _, err := m.Main.DB.GetPoll(ctx, pollID)
		if err != nil {
			zerolog.Ctx(ctx).Err(err).Int64("poll_id", pollID).Msg("Failed to look up poll")
		}
		return threadKey
	})
}

// prepareMessagePoll decides whether the poll card of a message is the poll or a repeat of it, and
// remembers which message the poll is. It is called before a message is converted.
func (m *MetaClient) prepareMessagePoll(ctx context.Context, msg *table.WrappedMessage) {
	if msg.Poll == nil {
		return
	}
	if !msg.Poll.IsCreatedBy(msg.MessageId) {
		msg.PollAlreadyBridged = true
		return
	}
	if _, _, ok := msgconv.PollFromTable(msg.Poll); !ok {
		return
	}
	threadKey := msg.Poll.ThreadKey
	if threadKey == 0 {
		threadKey = msg.ThreadKey
	}
	saved, err := m.Main.DB.PutPoll(ctx, msg.Poll.PollID, threadKey, string(metaid.MakeFBMessageID(msg.MessageId)))
	if err != nil {
		zerolog.Ctx(ctx).Err(err).Int64("poll_id", msg.Poll.PollID).Msg("Failed to save poll message")
	} else if saved != string(metaid.MakeFBMessageID(msg.MessageId)) {
		msg.PollAlreadyBridged = true
	}
}

// pollMessage finds the bridged message of a poll, and the part that has the poll.
func (m *MetaClient) pollMessage(ctx context.Context, pollID int64) (*database.Message, *metaid.PollMetadata, error) {
	_, messageID, err := m.Main.DB.GetPoll(ctx, pollID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to look up poll: %w", err)
	} else if messageID == "" {
		return nil, nil, nil
	}
	parts, err := m.Main.Bridge.DB.Message.GetAllPartsByID(ctx, m.UserLogin.ID, networkid.MessageID(messageID))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get poll message: %w", err)
	}
	for _, part := range parts {
		if meta, ok := part.Metadata.(*metaid.MessageMetadata); ok && meta.Poll != nil && meta.Poll.PollID == pollID {
			return part, meta.Poll, nil
		}
	}
	return nil, nil, nil
}

func (m *MetaClient) handlePollRow(tk handlerParams, row *pollRow) bridgev2.RemoteEvent {
	if row.Closed {
		return &pollEndEvent{
			Message: simplevent.Message[*pollRow]{
				EventMeta: simplevent.EventMeta{
					Type: bridgev2.RemoteEventMessage,
					LogContext: func(c zerolog.Context) zerolog.Context {
						return c.Str("action", "poll_end").Int64("poll_id", row.Poll.PollID)
					},
					PortalKey:         tk.Portal,
					UncertainReceiver: tk.IsUncertainReceiver(),
				},
				Data:               row,
				ID:                 networkid.MessageID(pollEndIDPrefix + strconv.FormatInt(row.Poll.PollID, 10)),
				ConvertMessageFunc: m.convertPollEnd,
			},
			m: m,
		}
	}
	ts := time.Now()
	if ms := row.maxTimestampMS(); ms > 0 {
		ts = time.UnixMilli(ms)
	}
	return &simplevent.Message[*pollRow]{
		EventMeta: simplevent.EventMeta{
			Type: bridgev2.RemoteEventMessage,
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.Str("action", "poll_vote").Int64("poll_id", row.Poll.PollID).Int64("voter_id", row.ContactID)
			},
			PortalKey:         tk.Portal,
			UncertainReceiver: tk.IsUncertainReceiver(),
			Sender:            m.makeEventSender(row.ContactID),
			Timestamp:         ts,
		},
		Data:               row,
		ID:                 makePollVoteID(row.Poll.PollID, row.ContactID, row.maxTimestampMS(), row.optionIDs()),
		ConvertMessageFunc: m.convertPollVote,
	}
}

func (m *MetaClient) savePoll(ctx context.Context, pollPart *database.Message) {
	if err := m.Main.Bridge.DB.Message.Update(ctx, pollPart); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to save poll state")
	}
}

func (m *MetaClient) convertPollVote(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, row *pollRow) (*bridgev2.ConvertedMessage, error) {
	pollPart, pm, err := m.pollMessage(ctx, row.Poll.PollID)
	if err != nil {
		return nil, err
	} else if pollPart == nil {
		zerolog.Ctx(ctx).Debug().Msg("Ignoring vote on a poll that hasn't been bridged")
		return nil, fmt.Errorf("%w: poll not bridged", bridgev2.ErrIgnoringRemoteEvent)
	}
	changed := false
	for _, opt := range row.Poll.Options {
		if pm.LearnOption(opt.OptionID, opt.OptionText) {
			changed = true
		}
	}
	answerIDs, voteChanged := pm.ApplyVote(row.ContactID, row.Votes)
	if changed || voteChanged {
		m.savePoll(ctx, pollPart)
	}
	if !voteChanged {
		return nil, fmt.Errorf("%w: vote didn't change", bridgev2.ErrIgnoringRemoteEvent)
	}
	content, extra := msgconv.PollResponseContent(pollPart.MXID, answerIDs)
	return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
		Type:       event.EventUnstablePollResponse,
		Content:    content,
		Extra:      extra,
		DBMetadata: &metaid.MessageMetadata{},
	}}}, nil
}

// pollEndEvent is the end of a poll that was closed on Messenger. Messenger doesn't say who closed
// it, and Matrix only accepts the end from whoever started the poll, so the event is sent by the
// sender of the poll's message, which is looked up once the event is handled.
type pollEndEvent struct {
	simplevent.Message[*pollRow]
	m *MetaClient
}

func (e *pollEndEvent) GetSender() bridgev2.EventSender {
	ctx := e.m.Main.Bridge.BackgroundCtx
	if pollPart, _, err := e.m.pollMessage(ctx, e.Data.Poll.PollID); err == nil && pollPart != nil {
		return e.m.makeEventSender(metaid.ParseUserID(pollPart.SenderID))
	}
	return e.m.selfEventSender()
}

func (m *MetaClient) convertPollEnd(ctx context.Context, portal *bridgev2.Portal, intent bridgev2.MatrixAPI, row *pollRow) (*bridgev2.ConvertedMessage, error) {
	pollPart, pm, err := m.pollMessage(ctx, row.Poll.PollID)
	if err != nil {
		return nil, err
	} else if pollPart == nil {
		return nil, fmt.Errorf("%w: poll not bridged", bridgev2.ErrIgnoringRemoteEvent)
	} else if pm.Closed {
		return nil, fmt.Errorf("%w: poll already closed", bridgev2.ErrIgnoringRemoteEvent)
	}
	pm.Closed = true
	m.savePoll(ctx, pollPart)
	content, extra := msgconv.PollEndContent(pollPart.MXID)
	return &bridgev2.ConvertedMessage{Parts: []*bridgev2.ConvertedMessagePart{{
		Type:       event.EventUnstablePollEnd,
		Content:    content,
		Extra:      extra,
		DBMetadata: &metaid.MessageMetadata{},
	}}}, nil
}

func pollUnsupported(err error) error {
	return bridgev2.WrapErrorInStatus(err).
		WithErrorReason(event.MessageStatusUnsupported).
		WithIsCertain(true).
		WithSendNotice(true).
		WithErrorAsMessage()
}

// pollFromCreateResponse finds the poll that a poll creation task made in the response to the task,
// and the message that has it.
func pollFromCreateResponse(tbl *table.LSTable, threadKey int64) (*table.WrappedPoll, string, error) {
	for _, failed := range tbl.LSHandleFailedTask {
		return nil, "", fmt.Errorf("%w: %s", ErrServerRejectedMessage, failed.Message)
	}
	polls := tbl.WrapPolls()
	var poll *table.WrappedPoll
	for _, candidate := range polls {
		if candidate.ThreadKey != 0 && candidate.ThreadKey != threadKey {
			continue
		}
		if poll == nil || candidate.PollID > poll.PollID {
			poll = candidate
		}
	}
	if poll == nil {
		return nil, "", fmt.Errorf("%w: response to poll creation didn't include the poll", ErrServerRejectedMessage)
	}
	_, insert := tbl.WrapMessages()
	for _, msg := range insert {
		if msg.Poll != nil && msg.Poll.PollID == poll.PollID {
			return poll, msg.MessageId, nil
		}
	}
	if poll.LastEventType == table.PollEventQuestionCreation && poll.LastEventMessageID != "" {
		return poll, poll.LastEventMessageID, nil
	}
	return poll, "", fmt.Errorf("%w: response to poll creation didn't say which message has the poll", ErrServerRejectedMessage)
}

func (m *MetaClient) pollPortalCheck(portal *bridgev2.Portal) error {
	if m.LoginMeta.Cookies == nil {
		return bridgev2.ErrNotLoggedIn
	}
	switch portal.Metadata.(*metaid.PortalMetadata).ThreadType {
	case table.ENCRYPTED_OVER_WA_ONE_TO_ONE, table.ENCRYPTED_OVER_WA_GROUP:
		return pollUnsupported(errors.New("polls are not supported in end-to-end encrypted chats"))
	}
	if m.LoginMeta.Platform.IsInstagram() {
		return pollUnsupported(errors.New("polls are not supported on Instagram"))
	}
	return nil
}

func (m *MetaClient) HandleMatrixPollStart(ctx context.Context, msg *bridgev2.MatrixPollStart) (*bridgev2.MatrixMessageResponse, error) {
	if err := m.pollPortalCheck(msg.Portal); err != nil {
		return nil, err
	}
	threadKey := metaid.ParseFBPortalID(msg.Portal.ID)
	task, pm, err := msgconv.MatrixPollToTask(threadKey, msg.Content)
	if err != nil {
		return nil, pollUnsupported(err)
	}
	if !m.connectWaiter.WaitTimeout(ConnectWaitTimeout) {
		return nil, ErrNotConnected
	}
	if err = m.Client.WaitUntilCanSendMessages(ctx, 15*time.Second); err != nil {
		return nil, err
	}
	resp, err := m.Client.ExecuteTasks(ctx, task)
	if err != nil {
		return nil, err
	}
	zerolog.Ctx(ctx).Trace().Any("response", resp).Msg("Meta poll creation response")
	poll, messageID, err := pollFromCreateResponse(resp, threadKey)
	if err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).Any("response", resp).Msg("Couldn't find the new poll in the response")
		return nil, err
	}
	pm.PollID = poll.PollID
	for _, opt := range poll.Options {
		pm.LearnOption(opt.OptionID, opt.OptionText)
	}
	fbMessageID := metaid.MakeFBMessageID(messageID)
	if saved, err := m.Main.DB.PutPoll(ctx, poll.PollID, threadKey, string(fbMessageID)); err != nil {
		zerolog.Ctx(ctx).Err(err).Msg("Failed to save poll message")
	} else if saved != string(fbMessageID) {
		zerolog.Ctx(ctx).Warn().Str("saved_message_id", saved).Str("new_message_id", string(fbMessageID)).Msg("Poll was already bridged from another message")
	}
	ts := time.Now()
	if parsed, err := methods.ParseMessageID(messageID); err == nil {
		ts = time.UnixMilli(parsed)
	}
	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        fbMessageID,
			SenderID:  networkid.UserID(m.UserLogin.ID),
			Timestamp: ts,
			Metadata:  &metaid.MessageMetadata{Poll: pm},
		},
		StreamOrder: ts.UnixMilli(),
	}, nil
}

// sendPollVote sets the vote of the logged in user on a poll and remembers it, so that the copy
// of the vote that Messenger sends back is not bridged again.
func (m *MetaClient) sendPollVote(ctx context.Context, portal *bridgev2.Portal, pollPart *database.Message, answers []string) (networkid.MessageID, error) {
	if err := m.pollPortalCheck(portal); err != nil {
		return "", err
	}
	meta, ok := pollPart.Metadata.(*metaid.MessageMetadata)
	if !ok || meta.Poll == nil {
		return "", pollUnsupported(errors.New("this message was bridged without poll data, so it can't be voted on"))
	}
	task, optionIDs, err := msgconv.MatrixVoteToTask(metaid.ParseFBPortalID(portal.ID), meta.Poll, answers)
	if err != nil {
		return "", pollUnsupported(err)
	}
	if !m.connectWaiter.WaitTimeout(ConnectWaitTimeout) {
		return "", ErrNotConnected
	}
	if err = m.Client.WaitUntilCanSendMessages(ctx, 15*time.Second); err != nil {
		return "", err
	}
	resp, err := m.Client.ExecuteTasks(ctx, task)
	if err != nil {
		return "", err
	}
	zerolog.Ctx(ctx).Trace().Any("response", resp).Msg("Meta poll vote response")
	for _, failed := range resp.LSHandleFailedTask {
		return "", fmt.Errorf("%w: %s", ErrServerRejectedMessage, failed.Message)
	}
	ownID := metaid.ParseUserLoginID(m.UserLogin.ID)
	meta.Poll.SetOwnVote(ownID, optionIDs)
	m.savePoll(ctx, pollPart)
	return makePollVoteID(meta.Poll.PollID, ownID, time.Now().UnixMilli(), optionIDs), nil
}

func (m *MetaClient) HandleMatrixPollVote(ctx context.Context, msg *bridgev2.MatrixPollVote) (*bridgev2.MatrixMessageResponse, error) {
	voteID, err := m.sendPollVote(ctx, msg.Portal, msg.VoteTo, msg.Content.Response.Answers)
	if err != nil {
		return nil, err
	}
	return &bridgev2.MatrixMessageResponse{
		DB: &database.Message{
			ID:        voteID,
			SenderID:  networkid.UserID(m.UserLogin.ID),
			Timestamp: time.Now(),
			Metadata:  &metaid.MessageMetadata{},
		},
	}, nil
}

// retractPollVote takes back the vote of a Matrix poll response that was redacted.
func (m *MetaClient) retractPollVote(ctx context.Context, msg *bridgev2.MatrixMessageRemove, pollID, voterID int64) error {
	if voterID != metaid.ParseUserLoginID(m.UserLogin.ID) {
		return pollUnsupported(errors.New("can't take back the vote of somebody else"))
	}
	pollPart, _, err := m.pollMessage(ctx, pollID)
	if err != nil {
		return err
	} else if pollPart == nil {
		return pollUnsupported(errors.New("the poll of the vote isn't known"))
	}
	_, err = m.sendPollVote(ctx, msg.Portal, pollPart, nil)
	return err
}
