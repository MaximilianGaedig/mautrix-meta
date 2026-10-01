package connector

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-meta/pkg/messagix"
)

func TestFriendRequestAllowance(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	ago := func(d time.Duration) int64 { return now.Add(-d).Unix() }

	tests := []struct {
		name      string
		sentAt    []int64
		wantKept  []int64
		wantWait  time.Duration
		wantLeft  int
		wantAllow bool
	}{
		{name: "never sent any", wantLeft: 5, wantAllow: true},
		{
			name:      "four in the last hour leaves one",
			sentAt:    []int64{ago(50 * time.Minute), ago(40 * time.Minute), ago(30 * time.Minute), ago(time.Minute)},
			wantKept:  []int64{ago(50 * time.Minute), ago(40 * time.Minute), ago(30 * time.Minute), ago(time.Minute)},
			wantLeft:  1,
			wantAllow: true,
		},
		{
			name:     "five in the last hour is the limit, and the wait is until the oldest leaves the window",
			sentAt:   []int64{ago(50 * time.Minute), ago(40 * time.Minute), ago(30 * time.Minute), ago(20 * time.Minute), ago(time.Minute)},
			wantKept: []int64{ago(50 * time.Minute), ago(40 * time.Minute), ago(30 * time.Minute), ago(20 * time.Minute), ago(time.Minute)},
			wantWait: 10 * time.Minute,
		},
		{
			name:      "requests older than an hour no longer count and are dropped",
			sentAt:    []int64{ago(3 * time.Hour), ago(2 * time.Hour), ago(61 * time.Minute), ago(time.Hour), ago(59 * time.Minute), ago(time.Minute)},
			wantKept:  []int64{ago(59 * time.Minute), ago(time.Minute)},
			wantLeft:  3,
			wantAllow: true,
		},
		{
			name:     "order in storage does not matter",
			sentAt:   []int64{ago(time.Minute), ago(20 * time.Minute), ago(55 * time.Minute), ago(30 * time.Minute), ago(40 * time.Minute)},
			wantKept: []int64{ago(55 * time.Minute), ago(40 * time.Minute), ago(30 * time.Minute), ago(20 * time.Minute), ago(time.Minute)},
			wantWait: 5 * time.Minute,
		},
		{
			name:     "a clock that went backwards does not open the limit",
			sentAt:   []int64{ago(-time.Hour), ago(-time.Hour), ago(-time.Hour), ago(-time.Hour), ago(-time.Hour)},
			wantKept: []int64{ago(-time.Hour), ago(-time.Hour), ago(-time.Hour), ago(-time.Hour), ago(-time.Hour)},
			wantWait: 2 * time.Hour,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kept, wait := friendRequestAllowance(tc.sentAt, now)
			if !slices.Equal(kept, tc.wantKept) {
				t.Errorf("kept = %v, want %v", kept, tc.wantKept)
			}
			if wait != tc.wantWait {
				t.Errorf("wait = %v, want %v", wait, tc.wantWait)
			}
			if allowed := wait == 0; allowed != tc.wantAllow {
				t.Errorf("allowed = %v, want %v", allowed, tc.wantAllow)
			}
			if tc.wantAllow {
				if left := FriendRequestsPerHour - len(kept); left != tc.wantLeft {
					t.Errorf("left = %d, want %d", left, tc.wantLeft)
				}
			}
		})
	}
}

func TestParseFriendTarget(t *testing.T) {
	parseGhost := func(mxid id.UserID) (networkid.UserID, bool) {
		local, ok := strings.CutPrefix(string(mxid), "@meta_")
		if !ok {
			return "", false
		}
		local, _, _ = strings.Cut(local, ":")
		return networkid.UserID(local), true
	}
	tests := []struct {
		arg     string
		want    networkid.UserID
		wantErr bool
	}{
		{arg: "100012345678901", want: "100012345678901"},
		{arg: "@meta_100012345678901:example.com", want: "100012345678901"},
		{arg: "https://matrix.to/#/@meta_100012345678901:example.com", want: "100012345678901"},
		{arg: "@alice:example.com", wantErr: true},
		{arg: "@meta_notanumber:example.com", wantErr: true},
		{arg: "max.mueller", wantErr: true},
		{arg: "0", wantErr: true},
		{arg: "-5", wantErr: true},
		{arg: "", wantErr: true},
	}
	for _, tc := range tests {
		got, err := parseFriendTarget(tc.arg, parseGhost)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseFriendTarget(%q) error = %v, wantErr %v", tc.arg, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("parseFriendTarget(%q) = %q, want %q", tc.arg, got, tc.want)
		}
	}
}

func TestSplitFriendArgs(t *testing.T) {
	for _, tc := range []struct {
		args  []string
		rest  []string
		again bool
	}{
		{nil, nil, false},
		{[]string{"123"}, []string{"123"}, false},
		{[]string{"123", "again"}, []string{"123"}, true},
		{[]string{"Again"}, []string{}, true},
		{[]string{"123", "456"}, []string{"123", "456"}, false},
	} {
		rest, again := splitFriendArgs(tc.args)
		if !slices.Equal(rest, tc.rest) || again != tc.again {
			t.Errorf("splitFriendArgs(%v) = %v, %v; want %v, %v", tc.args, rest, again, tc.rest, tc.again)
		}
	}
}

func TestFriendRequestOutcome(t *testing.T) {
	for status, want := range map[messagix.FriendshipStatus]FriendRequestOutcome{
		messagix.FriendshipOutgoingRequest: FriendRequestSent,
		messagix.FriendshipAreFriends:      FriendRequestNowFriends,
	} {
		if got := friendRequestOutcome(status); got != want {
			t.Errorf("friendRequestOutcome(%q) = %q, want %q", status, got, want)
		}
	}
}

func TestFriendRequestProfile(t *testing.T) {
	sent := friendRequestProfile(FriendRequestSent)
	if got := string(sent[FriendRequestProfileKey]); got != `"sent"` {
		t.Errorf("after a request went out, %s = %s, want \"sent\"", FriendRequestProfileKey, got)
	}
	if _, touched := sent[RelationshipProfileKey]; touched {
		t.Errorf("a pending request must not change %s: the network has not said they are a friend", RelationshipProfileKey)
	}
	friends := friendRequestProfile(FriendRequestNowFriends)
	if got := string(friends[RelationshipProfileKey]); got != `"friend"` {
		t.Errorf("after becoming friends, %s = %s, want \"friend\"", RelationshipProfileKey, got)
	}
	if got := string(friends[FriendRequestProfileKey]); got != `"accepted"` {
		t.Errorf("after becoming friends, %s = %s, want \"accepted\"", FriendRequestProfileKey, got)
	}
}

func TestFriendRequestBlockedByProfile(t *testing.T) {
	profile := func(kv ...string) database.ExtraProfile {
		ep := database.ExtraProfile{}
		for i := 0; i < len(kv); i += 2 {
			_ = ep.Set(kv[i], kv[i+1])
		}
		return ep
	}
	tests := []struct {
		name    string
		profile database.ExtraProfile
		again   bool
		want    error
	}{
		{name: "nothing known", profile: nil},
		{name: "asked before, and asking again on purpose", profile: profile(RelationshipProfileKey, "none", FriendRequestProfileKey, "sent"), again: true},
		{name: "a friend stays a friend however it is asked", profile: profile(RelationshipProfileKey, "friend"), again: true, want: ErrAlreadyFriends},
		{name: "not a contact", profile: profile(RelationshipProfileKey, "none")},
		{name: "a contact who is not a friend", profile: profile(RelationshipProfileKey, "contact")},
		{name: "already a friend", profile: profile(RelationshipProfileKey, "friend"), want: ErrAlreadyFriends},
		{name: "already asked", profile: profile(RelationshipProfileKey, "none", FriendRequestProfileKey, "sent"), want: ErrFriendRequestAlreadySent},
		{name: "asked, and since accepted", profile: profile(RelationshipProfileKey, "friend", FriendRequestProfileKey, "sent"), want: ErrAlreadyFriends},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := friendRequestBlockedByProfile(tc.profile, tc.again); !errors.Is(got, tc.want) || (tc.want == nil && got != nil) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFriendRequestFailureMessage(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{
		{messagix.ErrFriendingUnavailable, "cannot send friend requests"},
		{messagix.ErrFriendRequestRejected, "refused"},
		{messagix.ErrFriendRequestNotConfirmed, "did not confirm"},
		{ErrAlreadyFriends, "already friends"},
		{ErrFriendRequestAlreadySent, "friend <user> again"},
		{&FriendRequestRateLimitError{RetryAfter: 10 * time.Minute}, "10m"},
		{errors.New("boom"), "boom"},
	} {
		if got := friendRequestFailureMessage(tc.err); !strings.Contains(got, tc.want) {
			t.Errorf("message for %v = %q, want it to contain %q", tc.err, got, tc.want)
		}
	}
}
