package table

import "testing"

func TestContactViewerRelationshipName(t *testing.T) {
	for _, tc := range []struct {
		rel  ContactViewerRelationship
		want string
	}{
		{FACEBOOK_FRIEND, "friend"},
		{CONTACT_OF_VIEWER, "contact"},
		{SOFT_CONTACT, "suggested"},
		{NOT_CONTACT, "none"},
		// Unknown is the absence of an answer, not an answer. It has to stay empty, because the
		// caller writes the relationship into a profile only when this is non-empty - reporting
		// "none" here would claim the network said they are not a contact when it said nothing.
		{UNKNOWN_RELATIONSHIP, ""},
		{ContactViewerRelationship(99), ""},
	} {
		if got := tc.rel.Name(); got != tc.want {
			t.Errorf("ContactViewerRelationship(%d).Name() = %q, want %q", tc.rel, got, tc.want)
		}
	}
}

// Both contact rows carry the relationship, and the connector reaches them through one interface.
func TestBothContactRowsReportTheirRelationship(t *testing.T) {
	var _ interface {
		GetContactViewerRelationship() ContactViewerRelationship
	} = &LSDeleteThenInsertContact{}
	var _ interface {
		GetContactViewerRelationship() ContactViewerRelationship
	} = &LSVerifyContactRowExists{}

	if got := (&LSDeleteThenInsertContact{ContactViewerRelationship: FACEBOOK_FRIEND}).GetContactViewerRelationship(); got != FACEBOOK_FRIEND {
		t.Errorf("LSDeleteThenInsertContact reported %d", got)
	}
	if got := (&LSVerifyContactRowExists{ContactViewerRelationship: NOT_CONTACT}).GetContactViewerRelationship(); got != NOT_CONTACT {
		t.Errorf("LSVerifyContactRowExists reported %d", got)
	}
}
