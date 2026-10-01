package connector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/simplevent"
	"maunium.net/go/mautrix/event"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func TestParticipantNickname(t *testing.T) {
	m := testMetaClient()
	t.Run("a nickname is passed on", func(t *testing.T) {
		member := m.wrapChatMember(&table.LSAddParticipantIdToGroupThread{ThreadKey: 5, ContactId: 2000, Nickname: "Ali"})
		if member.Nickname == nil || *member.Nickname != "Ali" {
			t.Fatalf("nickname = %v", member.Nickname)
		}
		if member.Sender != "2000" || member.IsFromMe || member.Membership != event.MembershipJoin {
			t.Errorf("member = %+v", member)
		}
	})
	t.Run("no nickname is said out loud, so that a removed one goes away", func(t *testing.T) {
		member := m.wrapChatMember(&table.LSAddParticipantIdToGroupThread{ThreadKey: 5, ContactId: 2000})
		if member.Nickname == nil || *member.Nickname != "" {
			t.Fatalf("nickname = %v, want a pointer to an empty string", member.Nickname)
		}
	})
	t.Run("the nickname of the logged-in user is marked as theirs", func(t *testing.T) {
		// The bridge library does not rename a real Matrix user, and it tells by this flag.
		member := m.wrapChatMember(&table.LSAddParticipantIdToGroupThread{ThreadKey: 5, ContactId: 1000, Nickname: "Me"})
		if !member.IsFromMe {
			t.Errorf("member = %+v", member)
		}
	})
}

func TestLiveParticipantRowCarriesTheNickname(t *testing.T) {
	m := testMetaClient()
	row := &table.LSAddParticipantIdToGroupThread{ThreadKey: 5, ContactId: 2000, Nickname: "Ali"}

	t.Run("on its own it is a member change for that one participant", func(t *testing.T) {
		evt, ok := m.handleAddParticipant(handlerParams{ID: 5, Type: table.GROUP_THREAD}, row).(*simplevent.ChatInfoChange)
		if !ok || evt.ChatInfoChange == nil || evt.ChatInfoChange.MemberChanges == nil {
			t.Fatalf("event = %#v", evt)
		}
		members := evt.ChatInfoChange.MemberChanges.MemberMap
		member, ok := members["2000"]
		if len(members) != 1 || !ok || member.Nickname == nil || *member.Nickname != "Ali" {
			t.Fatalf("members = %+v", members)
		}
		if evt.ChatInfoChange.MemberChanges.IsFull {
			t.Error("one row is not the whole member list")
		}
	})
	t.Run("the same in a chat between two people", func(t *testing.T) {
		evt, ok := m.handleAddParticipant(handlerParams{ID: 2000, Type: table.ONE_TO_ONE}, row).(*simplevent.ChatInfoChange)
		if !ok || *evt.ChatInfoChange.MemberChanges.MemberMap["2000"].Nickname != "Ali" {
			t.Fatalf("event = %#v", evt)
		}
	})
	t.Run("with a thread sync in the same batch it becomes part of that", func(t *testing.T) {
		sync := &FBChatResync{Members: map[int64]bridgev2.ChatMember{}}
		if evt := m.handleAddParticipant(handlerParams{ID: 5, Type: table.GROUP_THREAD, Sync: sync}, row); evt != nil {
			t.Fatalf("event = %#v", evt)
		}
		if member := sync.Members[2000]; member.Nickname == nil || *member.Nickname != "Ali" {
			t.Fatalf("members = %+v", sync.Members)
		}
	})
}

func TestNicknameAdminTextIsKept(t *testing.T) {
	identity := func(k int64) int64 { return k }
	got := threadsWithStateChanges(&table.LSTable{
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{
			{ThreadKey: 1, ContactId: 2000},
			{ThreadKey: 2, ContactId: 2000, Nickname: "Ali"},
		},
	}, identity)
	if !got.Has(1) {
		t.Error("somebody joining is room state, the admin text would say it twice")
	}
	if got.Has(2) {
		t.Error("a nickname row is not a join: the admin text is the only thing saying who set it")
	}
}
