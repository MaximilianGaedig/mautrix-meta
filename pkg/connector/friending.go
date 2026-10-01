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
	"maunium.net/go/mautrix/bridgev2/commands"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

// FriendingNetworkAPI is an optional interface a network client implements when the network has a
// notion of asking somebody to be friends that is separate from messaging them.
//
// It is defined here only because mautrix-go is being changed elsewhere at the moment. It belongs
// in mautrix-go's bridgev2/networkinterface.go, beside the other optional NetworkAPI interfaces,
// together with FriendRequestResult and FriendRequestOutcome; nothing in it is Meta-specific, and
// the command below would then move to bridgev2/commands and work for every network that
// implements it.
type FriendingNetworkAPI interface {
	bridgev2.NetworkAPI
	// SendFriendRequest asks one user to be friends, once. It must only ever be called because the
	// Matrix user asked for exactly this, and implementations are expected to rate-limit it.
	SendFriendRequest(ctx context.Context, userID networkid.UserID) (*FriendRequestResult, error)
}

type FriendRequestOutcome string

const (
	// FriendRequestSent means a request is now waiting for the other person.
	FriendRequestSent FriendRequestOutcome = "sent"
	// FriendRequestNowFriends means the two are friends as a result: the other person had already
	// asked, so asking back accepted it.
	FriendRequestNowFriends FriendRequestOutcome = "accepted"
)

type FriendRequestResult struct {
	Outcome FriendRequestOutcome
	// Remaining is how many more requests the rate limit allows right now.
	Remaining int
}

var _ FriendingNetworkAPI = (*MetaClient)(nil)

// FriendRequestProfileKey is the request-in-flight half of how you know somebody, on the ghost's
// Matrix profile beside RelationshipProfileKey. A contact row can say friend or not a friend; it
// cannot say "asked", so that part is only ever known for requests made through this bridge:
// "sent" once one has gone out, "accepted" when sending one made the two friends. A client should
// read im.mxg.relationship first: "friend" there wins over a "sent" here, which is simply what a
// request that has since been accepted looks like.
const FriendRequestProfileKey = "im.mxg.friend_request"

const (
	// FriendRequestsPerHour is deliberately far below anything a person clicking around
	// facebook.com would be stopped at. Unsolicited friend requests from an automated client are
	// exactly what gets an account flagged, and nobody adds more than a handful of people in an
	// hour by hand.
	FriendRequestsPerHour = 5
	friendRequestWindow   = time.Hour
)

var (
	ErrAlreadyFriends           = errors.New("already friends")
	ErrFriendRequestAlreadySent = errors.New("a friend request was already sent")
	ErrFriendRequestToSelf      = errors.New("cannot send a friend request to yourself")
)

// FriendRequestRateLimitError is returned instead of sending when the hourly limit is used up.
type FriendRequestRateLimitError struct {
	RetryAfter time.Duration
}

func (e *FriendRequestRateLimitError) Error() string {
	return fmt.Sprintf("friend request limit reached (%d per hour), try again in %s", FriendRequestsPerHour, e.RetryAfter)
}

// friendRequestAllowance is the whole rate limit: given when requests were made (unix seconds, any
// order) and the time now, it returns the ones that still count, oldest first, and how long until
// another may be made - zero when one may be made now.
//
// Timestamps from the future are kept and counted. They can only come from a clock that has since
// been set back, and forgetting them would turn a clock change into a way around the limit.
func friendRequestAllowance(sentAt []int64, now time.Time) (kept []int64, retryAfter time.Duration) {
	cutoff := now.Add(-friendRequestWindow).Unix()
	for _, ts := range sentAt {
		if ts > cutoff {
			kept = append(kept, ts)
		}
	}
	slices.Sort(kept)
	if len(kept) < FriendRequestsPerHour {
		return kept, 0
	}
	// The slot that frees up first belongs to the oldest of the newest FriendRequestsPerHour.
	freesAt := time.Unix(kept[len(kept)-FriendRequestsPerHour], 0).Add(friendRequestWindow)
	return kept, max(freesAt.Sub(now), time.Second)
}

// parseFriendTarget turns what somebody typed after the command into a user on the network: the
// numeric Facebook user ID, or the Matrix ID of that user's ghost (bare or as a matrix.to link,
// which is what a mention pill turns into).
func parseFriendTarget(arg string, parseGhostMXID func(id.UserID) (networkid.UserID, bool)) (networkid.UserID, error) {
	arg = strings.TrimSpace(arg)
	if rest, ok := strings.CutPrefix(arg, "https://matrix.to/#/"); ok {
		arg = rest
	}
	if strings.HasPrefix(arg, "@") {
		ghostID, ok := parseGhostMXID(id.UserID(arg))
		if !ok {
			return "", fmt.Errorf("%s is not a user from this bridge", arg)
		}
		arg = string(ghostID)
	}
	userID, err := strconv.ParseInt(arg, 10, 64)
	if err != nil || userID <= 0 {
		return "", fmt.Errorf("%q is neither a numeric user ID nor the Matrix ID of a bridged user", arg)
	}
	return metaid.MakeUserID(userID), nil
}

func friendRequestOutcome(status messagix.FriendshipStatus) FriendRequestOutcome {
	if status == messagix.FriendshipAreFriends {
		return FriendRequestNowFriends
	}
	return FriendRequestSent
}

// friendRequestProfile is what a finished request changes on the ghost's profile. A request that
// is merely pending leaves the relationship alone: the network has not said they are a friend.
func friendRequestProfile(outcome FriendRequestOutcome) database.ExtraProfile {
	var profile database.ExtraProfile
	_ = profile.Set(FriendRequestProfileKey, string(outcome))
	if outcome == FriendRequestNowFriends {
		_ = profile.Set(RelationshipProfileKey, "friend")
	}
	return profile
}

// friendRequestBlockedByProfile says whether what is already known about somebody makes a request
// pointless, so that it costs neither a request to Meta nor one of the hour's few.
//
// "Already sent" is the bridge's own memory of having asked, and nothing ever tells it that the
// request was declined or withdrawn on facebook.com since. So that one can be overridden by asking
// again explicitly; being friends, which the network itself reports and keeps current, cannot.
func friendRequestBlockedByProfile(profile database.ExtraProfile, again bool) error {
	if string(profile[RelationshipProfileKey]) == `"friend"` {
		return ErrAlreadyFriends
	} else if !again && string(profile[FriendRequestProfileKey]) == `"`+string(FriendRequestSent)+`"` {
		return ErrFriendRequestAlreadySent
	}
	return nil
}

func (m *MetaClient) SendFriendRequest(ctx context.Context, userID networkid.UserID) (*FriendRequestResult, error) {
	targetID := metaid.ParseUserID(userID)
	if targetID <= 0 {
		return nil, fmt.Errorf("invalid user ID %q", userID)
	} else if userID == networkid.UserID(m.UserLogin.ID) {
		return nil, ErrFriendRequestToSelf
	} else if m.Client == nil {
		return nil, fmt.Errorf("%w: not connected", messagix.ErrFriendingUnavailable)
	}

	// One at a time per login: the limit is checked and spent under the lock, so two commands sent
	// together cannot both see the last free slot.
	m.friendRequestLock.Lock()
	defer m.friendRequestLock.Unlock()

	ghost, err := m.Main.Bridge.GetExistingGhostByID(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to look up ghost: %w", err)
	} else if ghost != nil {
		// Whether to repeat a request that was already made is the caller's question to ask the
		// user; by the time it gets here the answer was yes.
		if err = friendRequestBlockedByProfile(ghost.ExtraProfile, true); err != nil {
			return nil, err
		}
	}

	kept, retryAfter := friendRequestAllowance(m.LoginMeta.FriendRequestsSentAt, time.Now())
	if retryAfter > 0 {
		return nil, &FriendRequestRateLimitError{RetryAfter: retryAfter}
	}

	status, err := m.Client.SendFriendRequest(ctx, targetID)
	if errors.Is(err, messagix.ErrFriendingUnavailable) {
		// Nothing left this process, so nothing is spent.
		return nil, err
	}
	// Everything past this point reached Meta, and counts whether or not it worked: a string of
	// failing attempts looks no better from their side than a string of successful ones.
	m.LoginMeta.FriendRequestsSentAt = append(kept, time.Now().Unix())
	if saveErr := m.UserLogin.Save(ctx); saveErr != nil {
		zerolog.Ctx(ctx).Err(saveErr).Msg("Failed to save friend request history")
	}
	if err != nil {
		return nil, err
	}

	result := &FriendRequestResult{
		Outcome:   friendRequestOutcome(status),
		Remaining: FriendRequestsPerHour - len(m.LoginMeta.FriendRequestsSentAt),
	}
	// Not UpdateInfo: with no avatar in hand it would mark a ghost that has none yet as never
	// going to have one.
	if ghost != nil && ghost.UpdateContactInfo(ctx, nil, nil, friendRequestProfile(result.Outcome)) {
		if saveErr := m.Main.Bridge.DB.Ghost.Update(ctx, ghost.Ghost); saveErr != nil {
			zerolog.Ctx(ctx).Err(saveErr).Msg("Failed to save ghost after friend request")
		}
	}
	return result, nil
}

// friendRequestFailureMessage is what the person who typed the command is told when no request
// is pending afterwards. Never a bare "failed": each of these calls for something different.
func friendRequestFailureMessage(err error) string {
	var rateLimit *FriendRequestRateLimitError
	switch {
	case errors.As(err, &rateLimit):
		return fmt.Sprintf("Not sent: at most %d friend requests per hour go out through the bridge, to keep the account from being flagged. Try again in %s.",
			FriendRequestsPerHour, rateLimit.RetryAfter.Round(time.Minute))
	case errors.Is(err, ErrAlreadyFriends):
		return "Not sent: you are already friends."
	case errors.Is(err, ErrFriendRequestAlreadySent):
		return "Not sent: a friend request already went to this person through the bridge. If it was declined or withdrawn on facebook.com since, send another with `$cmdprefix friend <user> again`."
	case errors.Is(err, ErrFriendRequestToSelf):
		return "Not sent: that is you."
	case errors.Is(err, messagix.ErrFriendingUnavailable):
		return fmt.Sprintf("This login cannot send friend requests: friending is a facebook.com feature, not a Messenger one (%v).", err)
	case errors.Is(err, messagix.ErrFriendRequestRejected):
		return fmt.Sprintf("Facebook refused the friend request, so nothing was sent: %v", err)
	case errors.Is(err, messagix.ErrFriendRequestNotConfirmed):
		return fmt.Sprintf("Facebook did not confirm the friend request, so treat it as not sent and check on facebook.com: %v", err)
	default:
		return fmt.Sprintf("Failed to send the friend request: %v", err)
	}
}

// splitFriendArgs takes the trailing "again" off the arguments: the word that says a request already
// made through the bridge should be made once more.
func splitFriendArgs(args []string) (rest []string, again bool) {
	if len(args) > 0 && strings.EqualFold(args[len(args)-1], "again") {
		return args[:len(args)-1], true
	}
	return args, false
}

// replyFriendRequestFailure exists because Reply only fills in $cmdprefix in its format string, and
// these messages are not format strings: they carry error text from the network.
func replyFriendRequestFailure(ce *commands.Event, err error) {
	msg := strings.ReplaceAll(friendRequestFailureMessage(err), "$cmdprefix", ce.Bridge.Config.CommandPrefix)
	ce.ReplyAdvanced(msg, true, false)
}

var cmdFriend = &commands.FullHandler{
	Func: fnFriend,
	Name: "friend",
	Help: commands.HelpMeta{
		Section:     commands.HelpSectionChats,
		Description: fmt.Sprintf("Send a Facebook friend request to one person (at most %d per hour). In a private chat the other person is the default.", FriendRequestsPerHour),
		Args:        "[_user ID or ghost Matrix ID_] [again]",
	},
	RequiresLogin: true,
}

func fnFriend(ce *commands.Event) {
	args, again := splitFriendArgs(ce.Args)
	var target networkid.UserID
	switch {
	case len(args) > 1:
		ce.Reply("One person at a time: `$cmdprefix friend <user ID or ghost Matrix ID>`")
		return
	case len(args) == 1:
		var err error
		target, err = parseFriendTarget(args[0], ce.Bridge.Matrix.ParseGhostMXID)
		if err != nil {
			ce.Reply("Not sent: %v", err)
			return
		}
	case ce.Portal != nil && ce.Portal.RoomType == database.RoomTypeDM && ce.Portal.OtherUserID != "":
		target = ce.Portal.OtherUserID
	default:
		ce.Reply("Usage: `$cmdprefix friend <user ID or ghost Matrix ID>`, or without arguments in a private chat")
		return
	}

	var login *bridgev2.UserLogin
	if ce.Portal != nil {
		login, _, _ = ce.Portal.FindPreferredLogin(ce.Ctx, ce.User, false)
	}
	if login == nil {
		login = ce.User.GetDefaultLogin()
	}
	if login == nil {
		ce.Reply("You are not logged in")
		return
	}
	api, ok := login.Client.(FriendingNetworkAPI)
	if !ok {
		ce.Reply("This login cannot send friend requests")
		return
	}

	who := fmt.Sprintf("user %s", target)
	if ghost, _ := ce.Bridge.GetExistingGhostByID(ce.Ctx, target); ghost != nil {
		if ghost.Name != "" {
			who = fmt.Sprintf("%s (%s)", ghost.Name, target)
		}
		if err := friendRequestBlockedByProfile(ghost.ExtraProfile, again); err != nil {
			replyFriendRequestFailure(ce, err)
			return
		}
	}

	result, err := api.SendFriendRequest(ce.Ctx, target)
	if err != nil {
		ce.Log.Err(err).Str("target_user_id", string(target)).Msg("Friend request not sent")
		replyFriendRequestFailure(ce, err)
		return
	}
	switch result.Outcome {
	case FriendRequestNowFriends:
		ce.Reply("You and %s are now friends on Facebook: they had already sent you a request, and this accepted it. %d more friend requests can be sent this hour.", who, result.Remaining)
	default:
		ce.Reply("Friend request sent to %s as %s. It is now waiting for them on Facebook. %d more can be sent this hour.", who, login.RemoteName, result.Remaining)
	}
}
