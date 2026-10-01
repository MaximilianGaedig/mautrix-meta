package httpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/go-querystring/query"

	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// MercuryThreadStatus is a request of facebook.com's older chat code ("Mercury") that turns a flag of a thread
// on or off. The LightSpeed socket has a task to mark a thread read, but none was found for marking it unread,
// while MercuryServerRequests in the web client's bundle still has an endpoint for both directions.
//
// These requests are built from that code and were never seen on the wire: the endpoints may no longer answer
// for a user's inbox, in which case the request fails and nothing changes.
type MercuryThreadStatus struct {
	// Endpoint is the name of the endpoint in the endpoints package.
	Endpoint string
	ThreadID int64
	Value    bool
	// NoValue are the arguments the web client sends next to the flag, in its order. They are sent as it
	// sends an argument it has no value for: the name alone.
	NoValue []string
}

// NewMercuryReadStatus is the request of MercuryServerRequests.changeThreadReadStatus, whose body is
// {ids: {<thread>: <read>}, source, watermarkTimestamp, shouldSendReadReceipt}. The last three are the ones
// the web client itself passes null for when it marks a thread read without a watermark.
func NewMercuryReadStatus(threadID int64, read bool) *MercuryThreadStatus {
	return &MercuryThreadStatus{
		Endpoint: "mercury_change_read_status",
		ThreadID: threadID,
		Value:    read,
		NoValue:  []string{"source", "watermarkTimestamp", "shouldSendReadReceipt"},
	}
}

// NewMercuryArchivedStatus is the request of MercuryServerRequests.changeThreadArchivedStatus, whose body is
// {ids: {<thread>: <archived>}, source}.
func NewMercuryArchivedStatus(threadID int64, archived bool) *MercuryThreadStatus {
	return &MercuryThreadStatus{
		Endpoint: "mercury_change_archived_status",
		ThreadID: threadID,
		Value:    archived,
		NoValue:  []string{"source"},
	}
}

// Form is the request body as the web client's PHPQuerySerializer writes it: the nested ids object becomes
// ids[<thread>], with the brackets left unescaped, and a null argument is only its name.
func (s *MercuryThreadStatus) Form() string {
	parts := make([]string, 0, 1+len(s.NoValue))
	parts = append(parts, "ids["+strconv.FormatInt(s.ThreadID, 10)+"]="+strconv.FormatBool(s.Value))
	parts = append(parts, s.NoValue...)
	return strings.Join(parts, "&")
}

// SendMercuryThreadStatus posts the status change with the parameters every request of the web client carries.
func (c *HTTPClient) SendMercuryThreadStatus(ctx context.Context, status *MercuryThreadStatus) error {
	if c == nil {
		return fmt.Errorf("client is nil")
	}
	common, err := query.Values(c.NewHTTPQuery())
	if err != nil {
		return fmt.Errorf("failed to convert HttpQuery into query.Values for mercury status change: %w", err)
	}
	payload := []byte(status.Form() + "&" + common.Encode())

	h := c.BuildHeaders(true, false)
	h.Set("accept", "*/*")
	h.Set("origin", c.parent.GetEndpoint("base_url"))
	h.Set("referer", c.parent.GetEndpoint("messages")+"/")
	h.Set("sec-fetch-dest", "empty")
	h.Set("sec-fetch-mode", "cors")
	h.Set("sec-fetch-site", "same-origin")

	_, respBody, err := c.MakeRequest(ctx, c.parent.GetEndpoint(status.Endpoint), http.MethodPost, h, payload, types.FORM)
	if err != nil {
		return fmt.Errorf("failed to send mercury status change: %w", err)
	}
	return parseMercuryStatusResponse(respBody)
}

// parseMercuryStatusResponse finds the error in the answer to a status change. A success carries nothing the
// bridge needs: the change comes back over the socket like one made on another device.
func parseMercuryStatusResponse(respBody []byte) error {
	var resp types.ErrorResponse
	if err := json.Unmarshal(bytes.TrimPrefix(respBody, AntiJSPrefix), &resp); err != nil {
		return fmt.Errorf("failed to parse mercury status response: %w", err)
	} else if resp.ErrorCode != 0 {
		return fmt.Errorf("error in mercury status change: %w", &resp)
	}
	return nil
}
