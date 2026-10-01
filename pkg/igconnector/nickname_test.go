package igconnector

import (
	"testing"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

func TestThreadNicknames(t *testing.T) {
	t.Run("a listed member has the nickname", func(t *testing.T) {
		nick := threadNicknames([]slidetypes.Nickname{{EIMUID: 7, Nickname: "Ali"}})(7)
		if nick == nil || *nick != "Ali" {
			t.Fatalf("nickname = %v", nick)
		}
	})
	t.Run("a member the list leaves out has none, which is said out loud", func(t *testing.T) {
		for _, list := range [][]slidetypes.Nickname{{{EIMUID: 7, Nickname: "Ali"}}, {}} {
			nick := threadNicknames(list)(8)
			if nick == nil || *nick != "" {
				t.Fatalf("nickname = %v, want a pointer to an empty string", nick)
			}
		}
	})
	t.Run("a thread without the list says nothing", func(t *testing.T) {
		if nick := threadNicknames(nil)(7); nick != nil {
			t.Fatalf("nickname = %q, want nil", *nick)
		}
	})
}
