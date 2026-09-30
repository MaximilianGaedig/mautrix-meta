package textfmt

import (
	"fmt"
	"strconv"
	"strings"

	"maunium.net/go/mautrix/event"
)

// ParseGeoURI reads the coordinates of an RFC 5870 geo: URI. Anything after a semicolon, such as the
// uncertainty, is ignored.
func ParseGeoURI(uri string) (lat, long float64, err error) {
	if !strings.HasPrefix(uri, "geo:") {
		err = fmt.Errorf("uri doesn't have geo: prefix")
		return
	}
	coordinates := strings.Split(strings.TrimPrefix(uri, "geo:"), ";")[0]

	if splitCoordinates := strings.Split(coordinates, ","); len(splitCoordinates) < 2 || len(splitCoordinates) > 3 {
		err = fmt.Errorf("didn't find two numbers separated by a comma")
	} else if lat, err = strconv.ParseFloat(splitCoordinates[0], 64); err != nil {
		err = fmt.Errorf("latitude is not a number: %w", err)
	} else if long, err = strconv.ParseFloat(splitCoordinates[1], 64); err != nil {
		err = fmt.Errorf("longitude is not a number: %w", err)
	}
	return
}

// LocationText is a Matrix location message as a text message, for the chats where Messenger and Instagram have
// no way of sending a location: what the sender wrote, then a link to the place on a map.
func LocationText(content *event.MessageEventContent) (string, error) {
	lat, long, err := ParseGeoURI(content.GeoURI)
	if err != nil {
		return "", fmt.Errorf("invalid location: %w", err)
	}
	link := fmt.Sprintf("https://www.google.com/maps?q=%.6f,%.6f", lat, long)
	body := strings.TrimSpace(content.Body)
	if body == "" || strings.HasPrefix(body, "geo:") {
		return link, nil
	}
	return body + "\n" + link, nil
}
