package igconnector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"
)

func TestAdminChange(t *testing.T) {
	for _, tc := range []struct{ orig, new, want int }{
		{0, powerAdmin, 1},
		{0, 100, 1},
		{powerAdmin, 0, -1},
		{powerAdmin, 100, 0},
		{0, powerAdmin - 1, 0},
	} {
		got := adminChange(&bridgev2.SinglePowerLevelChange{OrigLevel: tc.orig, NewLevel: tc.new, NewIsSet: true})
		if got != tc.want {
			t.Errorf("%d → %d: got %d, want %d", tc.orig, tc.new, got, tc.want)
		}
	}
}
