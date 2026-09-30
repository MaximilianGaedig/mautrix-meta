package connector

import (
	"testing"

	"maunium.net/go/mautrix/bridgev2"
)

func TestAdminTaskFor(t *testing.T) {
	change := func(orig, new int) *bridgev2.SinglePowerLevelChange {
		return &bridgev2.SinglePowerLevelChange{OrigLevel: orig, NewLevel: new, NewIsSet: true}
	}
	if task := adminTaskFor(1, 2, change(0, 100)); task == nil || task.IsAdmin != 1 || task.ThreadKey != 1 || task.ContactID != 2 {
		t.Errorf("promote = %+v", task)
	}
	if task := adminTaskFor(1, 2, change(fbPowerAdmin, 0)); task == nil || task.IsAdmin != 0 {
		t.Errorf("demote = %+v", task)
	}
	for _, c := range []*bridgev2.SinglePowerLevelChange{
		change(fbPowerAdmin, 100),    // still an admin
		change(0, 10),                // still not one
		change(fbPowerSuperAdmin, 0), // the group's creator
	} {
		if task := adminTaskFor(1, 2, c); task != nil {
			t.Errorf("%+v: got %+v, want no task", c, task)
		}
	}
}
