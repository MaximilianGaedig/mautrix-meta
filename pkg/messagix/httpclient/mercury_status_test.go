package httpclient

import (
	"errors"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// The web client's MercuryServerRequests.changeThreadReadStatus(thread, read, null, null, null) sends
// {ids: {<fbid>: read}, source: null, watermarkTimestamp: null, shouldSendReadReceipt: null}, which its
// PHPQuerySerializer writes like this.
func TestMercuryReadStatusFormIsTheWebClients(t *testing.T) {
	if got, want := NewMercuryReadStatus(4242, false).Form(), "ids[4242]=false&source&watermarkTimestamp&shouldSendReadReceipt"; got != want {
		t.Errorf("mark unread:\n got %s\nwant %s", got, want)
	}
	if got, want := NewMercuryReadStatus(4242, true).Form(), "ids[4242]=true&source&watermarkTimestamp&shouldSendReadReceipt"; got != want {
		t.Errorf("mark read:\n got %s\nwant %s", got, want)
	}
	if got := NewMercuryReadStatus(4242, false).Endpoint; got != "mercury_change_read_status" {
		t.Errorf("endpoint = %q", got)
	}
}

// MercuryServerRequests.changeThreadArchivedStatus sends {ids: {<fbid>: archived}, source}.
func TestMercuryArchivedStatusFormIsTheWebClients(t *testing.T) {
	status := NewMercuryArchivedStatus(4242, false)
	if got, want := status.Form(), "ids[4242]=false&source"; got != want {
		t.Errorf("unarchive:\n got %s\nwant %s", got, want)
	}
	if status.Endpoint != "mercury_change_archived_status" {
		t.Errorf("endpoint = %q", status.Endpoint)
	}
}

func TestMercuryStatusResponse(t *testing.T) {
	if err := parseMercuryStatusResponse([]byte(`for (;;);{"__ar":1,"payload":null,"lid":"1"}`)); err != nil {
		t.Errorf("a plain answer is a success: %v", err)
	}
	err := parseMercuryStatusResponse([]byte(`for (;;);{"__ar":1,"error":1357004,"errorSummary":"Sorry, something went wrong"}`))
	if !errors.Is(err, types.ErrPleaseReloadPage) {
		t.Errorf("an error answer must be returned as its error: %v", err)
	}
	if err = parseMercuryStatusResponse([]byte(`<!DOCTYPE html><html>`)); err == nil {
		t.Error("an answer that is not JSON, like the page of an endpoint that is gone, must be an error")
	}
}
