package messagix

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// Friending is not a Messenger operation. It is a facebook.com GraphQL mutation, sent to the same
// /api/graphql/ endpoint and with the same fb_dtsg as everything else the www session does, so it
// only exists for logins that hold a facebook.com session.
//
// Where this comes from: facebook.com's own bundle as served on 2026-09-19 (the modules
// FriendingCometFriendRequestSendMutation, FriendingCometFriendRequestSendMutation.graphql and
// RelayFBCometMutations). The doc id, the shape of the variables and the selection the response
// follows are read from there. What is NOT from a capture is a request actually made: nobody has
// recorded one, so the two values the web client fills from its own navigation state
// (attribution_id_v2 and click_correlation_id) are sent as null rather than invented, and this has
// never been run against the live server.

const friendRequestSendDoc = "FriendingCometFriendRequestSendMutation"

// friendingChannel says where in the product the request was made. It is an enum on Meta's side;
// PROFILE_BUTTON is the value the web client uses for the button on somebody's profile, which is
// the closest thing to what a person asking for one particular user is doing.
const friendingChannel = "PROFILE_BUTTON"

var (
	// ErrFriendingUnavailable means this login cannot send friend requests at all, as opposed to
	// one request having failed.
	ErrFriendingUnavailable = errors.New("friend requests are not available for this login")
	// ErrFriendRequestRejected means the server answered with an error. A rotated doc id looks
	// like this too.
	ErrFriendRequestRejected = errors.New("friend request was rejected by the server")
	// ErrFriendRequestNotConfirmed means the request was made and nothing in the answer says a
	// request is now pending, so it must not be reported as sent.
	ErrFriendRequestNotConfirmed = errors.New("friend request was not confirmed by the server")
)

// FriendshipStatus is facebook.com's own word for how the viewer and a user stand, which is finer
// than what a Messenger contact row says: it knows about requests in flight.
type FriendshipStatus string

const (
	FriendshipAreFriends      FriendshipStatus = "ARE_FRIENDS"
	FriendshipCanRequest      FriendshipStatus = "CAN_REQUEST"
	FriendshipIncomingRequest FriendshipStatus = "INCOMING_REQUEST"
	FriendshipOutgoingRequest FriendshipStatus = "OUTGOING_REQUEST"
)

type friendRequestSendInput struct {
	AttributionIDV2          *string  `json:"attribution_id_v2"`
	ClickCorrelationID       *string  `json:"click_correlation_id"`
	ClickProofValidation     *string  `json:"click_proof_validation_result"`
	ExtraData                *string  `json:"extra_data"`
	FriendRequesteeIDs       []string `json:"friend_requestee_ids"`
	FriendingChannel         string   `json:"friending_channel"`
	PeopleYouMayKnowLocation *string  `json:"people_you_may_know_location"`
	WarnAckForIDs            []string `json:"warn_ack_for_ids"`
	ActorID                  string   `json:"actor_id"`
	ClientMutationID         string   `json:"client_mutation_id"`
}

type friendRequestSendVariables struct {
	Input friendRequestSendInput `json:"input"`
	Scale float64                `json:"scale"`
}

// newFriendRequestSendVariables builds the variables exactly one user at a time: the mutation takes
// a list, and sending a list is how a client would batch, which this must never do.
//
// click_proof_validation_result stays null on purpose. The web client sets it to {"validated":true}
// when a real click was verified, and no click happened here.
func newFriendRequestSendVariables(actorID string, userID int64, mutationID int64, scale float64) *friendRequestSendVariables {
	if scale <= 0 {
		scale = 1
	}
	return &friendRequestSendVariables{
		Input: friendRequestSendInput{
			FriendRequesteeIDs: []string{strconv.FormatInt(userID, 10)},
			FriendingChannel:   friendingChannel,
			WarnAckForIDs:      []string{},
			ActorID:            actorID,
			ClientMutationID:   strconv.FormatInt(mutationID, 10),
		},
		Scale: scale,
	}
}

type friendRequestSendResponse struct {
	Data struct {
		FriendRequestSend *struct {
			FriendRequestees []struct {
				ID               string           `json:"id"`
				FriendshipStatus FriendshipStatus `json:"friendship_status"`
			} `json:"friend_requestees"`
		} `json:"friend_request_send"`
	} `json:"data"`
	types.ErrorResponse
}

// parseFriendRequestSendResponse reads what the mutation answered about one user. It only reports
// success when the server itself says a request is now outgoing, or that the two are now friends
// (which is what sending to somebody who had already asked does). Anything else is an error, with
// the status alongside when there was one.
func parseFriendRequestSendResponse(body []byte, userID int64) (FriendshipStatus, error) {
	var resp friendRequestSendResponse
	// Only the first JSON value: a response can be followed by further chunks on later lines.
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&resp); err != nil {
		return "", fmt.Errorf("%w: unreadable response: %w", ErrFriendRequestNotConfirmed, err)
	}
	if err := resp.ErrorResponse.AsError(); err != nil {
		return "", fmt.Errorf("%w: %w", ErrFriendRequestRejected, err)
	}
	if resp.Data.FriendRequestSend == nil {
		return "", fmt.Errorf("%w: the response has no result", ErrFriendRequestNotConfirmed)
	}
	wantID := strconv.FormatInt(userID, 10)
	for _, requestee := range resp.Data.FriendRequestSend.FriendRequestees {
		if requestee.ID != wantID {
			continue
		}
		switch requestee.FriendshipStatus {
		case FriendshipOutgoingRequest, FriendshipAreFriends:
			return requestee.FriendshipStatus, nil
		default:
			return requestee.FriendshipStatus, fmt.Errorf("%w: status is %q", ErrFriendRequestNotConfirmed, requestee.FriendshipStatus)
		}
	}
	return "", fmt.Errorf("%w: the response does not mention user %s", ErrFriendRequestNotConfirmed, wantID)
}

// friendingAvailableOn says whether a login on this platform has what the mutation needs: a
// facebook.com www session. messenger.com is a different site with its own cookies and has never
// been seen to serve this mutation; Messenger Lite has no www session at all; Instagram has no
// friends.
func friendingAvailableOn(platform types.Platform) error {
	if platform.IsViaFacebook() {
		return nil
	}
	name := platform.String()
	if name == "" {
		name = "unknown"
	}
	return fmt.Errorf("%w: it needs a facebook.com session and this one is %s", ErrFriendingUnavailable, name)
}

// SendFriendRequest asks one user to be friends, as the logged-in account. It makes exactly one
// request and never retries: a second identical mutation is at best a no-op and at worst looks
// like automation. Deciding whether and how often to call this is the caller's job.
func (c *Client) SendFriendRequest(ctx context.Context, userID int64) (FriendshipStatus, error) {
	if c == nil {
		return "", ErrClientIsNil
	} else if err := friendingAvailableOn(c.GetPlatform()); err != nil {
		return "", err
	} else if !c.IsAuthenticated() {
		return "", fmt.Errorf("%w: not logged in", ErrFriendingUnavailable)
	} else if userID <= 0 {
		return "", fmt.Errorf("invalid user id %d", userID)
	}
	actorID := c.configs.BrowserConfigTable.CurrentUserInitialData.UserID
	if actorID == "" || actorID == "0" {
		return "", fmt.Errorf("%w: own user id is not known", ErrFriendingUnavailable)
	}
	variables := newFriendRequestSendVariables(actorID, userID, c.friendingMutationID.Add(1), c.configs.BrowserConfigTable.SiteData.Pr)
	_, respData, err := c.http.MakeGraphQLRequest(ctx, friendRequestSendDoc, variables)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	status, err := parseFriendRequestSendResponse(respData, userID)
	if err != nil {
		logData := respData
		if len(logData) > 4096 {
			logData = logData[:4096]
		}
		c.Logger.Debug().Str("resp_data", string(logData)).Int64("target_user_id", userID).Msg("Response data for failed friend request")
	}
	return status, err
}
