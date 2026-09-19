// mautrix-meta - A Matrix-Facebook Messenger and Instagram DM puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package messagix

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestBXDataURI(t *testing.T) {
	page := `<script type="application/json" data-sjs>{"require":[["HasteSupportData","handle",null,[{"bxData":{"12":{"uri":"https:\/\/static.xx.fbcdn.net\/rsrc.php\/a.wasm"}},"clpData":{}}]]]}</script>` +
		`<script>{"bxData":{"5":{"uri":"https:\/\/static.xx.fbcdn.net\/rsrc.php\/b.wasm"},"37":{"uri":"https:\/\/static.xx.fbcdn.net\/rsrc.php\/yV\/r\/Wfd6t0e74Jr.wasm"}}}</script>`
	if got := BXDataURI([]byte(page), "37"); got != "https://static.xx.fbcdn.net/rsrc.php/yV/r/Wfd6t0e74Jr.wasm" {
		t.Errorf("entry 37: %q", got)
	}
	if got := BXDataURI([]byte(page), "12"); got != "https://static.xx.fbcdn.net/rsrc.php/a.wasm" {
		t.Errorf("entry 12: %q", got)
	}
	if got := BXDataURI([]byte(page), "3"); got != "" {
		t.Errorf("missing entry: %q", got)
	}
}

// TestBXDataURIGroupCallPage reads the frame-encryption module's URL from the group-call page of the
// capture FRAMECRYPT_HAR names.
func TestBXDataURIGroupCallPage(t *testing.T) {
	path := os.Getenv("FRAMECRYPT_HAR")
	if path == "" {
		t.Skip("FRAMECRYPT_HAR not set")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var har struct {
		Log struct {
			Entries []struct {
				Request struct {
					URL string `json:"url"`
				} `json:"request"`
				Response struct {
					Content struct {
						Text string `json:"text"`
					} `json:"content"`
				} `json:"response"`
			} `json:"entries"`
		} `json:"log"`
	}
	if err = json.Unmarshal(raw, &har); err != nil {
		t.Fatal(err)
	}
	var wasmURLs []string
	var got string
	for _, e := range har.Log.Entries {
		if strings.HasSuffix(e.Request.URL, ".wasm") {
			wasmURLs = append(wasmURLs, e.Request.URL)
		}
		if strings.Contains(e.Request.URL, "/groupcall/") {
			got = BXDataURI([]byte(e.Response.Content.Text), FrameEncryptionResourceID)
		}
	}
	if got == "" {
		t.Fatal("no frame-encryption module URL on the captured group-call page")
	}
	for _, u := range wasmURLs {
		if u == got {
			t.Logf("page names %s, which the web client fetched", got)
			return
		}
	}
	t.Fatalf("page names %s, but the web client fetched %v", got, wasmURLs)
}
