package messagix

import (
	"encoding/json"
	"errors"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/graphql"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

func TestFriendRequestSendVariables(t *testing.T) {
	got, err := json.Marshal(newFriendRequestSendVariables("100", 200, 7, 1.5))
	if err != nil {
		t.Fatal(err)
	}
	// Key for key what facebook.com's FriendingCometFriendRequestSendMutation.commit builds, plus the
	// actor_id and client_mutation_id RelayFBCometMutations adds to every mutation input.
	const want = `{"input":{"attribution_id_v2":null,"click_correlation_id":null,"click_proof_validation_result":null,` +
		`"extra_data":null,"friend_requestee_ids":["200"],"friending_channel":"PROFILE_BUTTON",` +
		`"people_you_may_know_location":null,"warn_ack_for_ids":[],"actor_id":"100","client_mutation_id":"7"},"scale":1.5}`
	if string(got) != want {
		t.Errorf("variables differ from the web client's\n got %s\nwant %s", got, want)
	}
}

func TestFriendRequestSendDocIsRegistered(t *testing.T) {
	doc, ok := graphql.GraphQLDocs[friendRequestSendDoc]
	if !ok || doc.DocID == "" || doc.FriendlyName != "FriendingCometFriendRequestSendMutation" {
		t.Fatalf("send mutation is not registered properly: %+v (found %v)", doc, ok)
	}
}

func TestParseFriendRequestSendResponse(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		want    FriendshipStatus
		wantErr error
	}{
		{
			name: "request went out",
			body: `{"data":{"friend_request_send":{"friend_requestees":[{"id":"200","friendship_status":"OUTGOING_REQUEST"}]}},"extensions":{"is_final":true}}`,
			want: FriendshipOutgoingRequest,
		},
		{
			name: "they had already asked us, so it made us friends",
			body: `{"data":{"friend_request_send":{"friend_requestees":[{"id":"200","friendship_status":"ARE_FRIENDS"}]}}}`,
			want: FriendshipAreFriends,
		},
		{
			name: "a second chunk after the first is ignored",
			body: `{"data":{"friend_request_send":{"friend_requestees":[{"id":"200","friendship_status":"OUTGOING_REQUEST"}]}}}` + "\n" + `{"label":"x","path":[],"data":{}}`,
			want: FriendshipOutgoingRequest,
		},
		{
			name:    "the answer is about somebody else",
			body:    `{"data":{"friend_request_send":{"friend_requestees":[{"id":"999","friendship_status":"OUTGOING_REQUEST"}]}}}`,
			wantErr: ErrFriendRequestNotConfirmed,
		},
		{
			name:    "nothing came back",
			body:    `{"data":{"friend_request_send":null}}`,
			wantErr: ErrFriendRequestNotConfirmed,
		},
		{
			name:    "refused, and the status says it is still only possible",
			body:    `{"data":{"friend_request_send":{"friend_requestees":[{"id":"200","friendship_status":"CAN_REQUEST"}]}}}`,
			want:    FriendshipCanRequest,
			wantErr: ErrFriendRequestNotConfirmed,
		},
		{
			name:    "graphql errors",
			body:    `{"data":{"friend_request_send":null},"errors":[{"message":"A server error","severity":"CRITICAL","code":1675030,"summary":"Query error","description":"Error performing query."}]}`,
			wantErr: ErrFriendRequestRejected,
		},
		{
			name:    "top-level error, such as a rotated doc id",
			body:    `{"error":1357004,"errorSummary":"Sorry, something went wrong","errorDescription":"Please try closing and re-opening your browser window."}`,
			wantErr: ErrFriendRequestRejected,
		},
		{
			name:    "not json",
			body:    `<html>`,
			wantErr: ErrFriendRequestNotConfirmed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFriendRequestSendResponse([]byte(tc.body), 200)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("status = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFriendingAvailability(t *testing.T) {
	for platform, want := range map[types.Platform]bool{
		types.Facebook:             true,
		types.FacebookTor:          true,
		types.Messenger:            false,
		types.MessengerLiteIOS:     false,
		types.MessengerLiteAndroid: false,
		types.Instagram:            false,
		types.Unset:                false,
	} {
		if got := friendingAvailableOn(platform) == nil; got != want {
			t.Errorf("friending available on %s = %v, want %v", platform, got, want)
		}
		if err := friendingAvailableOn(platform); err != nil && !errors.Is(err, ErrFriendingUnavailable) {
			t.Errorf("%s: error %v is not ErrFriendingUnavailable", platform, err)
		}
	}
}
