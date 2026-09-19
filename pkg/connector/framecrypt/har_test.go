package framecrypt

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/rtcsignal"
)

// harE2ee is the E2EE material found in a captured call: the E2eeState
// state-sync blobs and the E2eeKey data messages, in capture order.
type harE2ee struct {
	States []harBlob
	Keys   []harBlob
}

type harBlob struct {
	Dir    string // "send" or "receive"
	Sender string // DataMessage.Sender (E2eeKey only)
	Data   []byte
}

// loadHARE2ee decodes the HAR named by FRAMECRYPT_HAR (same format as the
// rtcsignal HAR tests) and collects its E2EE blobs. It skips when unset.
func loadHARE2ee(t *testing.T) *harE2ee {
	t.Helper()
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
				WebSocketMessages []struct {
					Type   string `json:"type"`
					Opcode int    `json:"opcode"`
					Data   string `json:"data"`
				} `json:"_webSocketMessages"`
			} `json:"entries"`
		} `json:"log"`
	}
	if err = json.Unmarshal(raw, &har); err != nil {
		t.Fatal(err)
	}
	out := &harE2ee{}
	for _, e := range har.Log.Entries {
		if !strings.Contains(e.Request.URL, rtcsignal.RPSignalingPath) {
			continue
		}
		for _, m := range e.WebSocketMessages {
			if m.Opcode != 2 {
				continue
			}
			data, err := base64.StdEncoding.DecodeString(m.Data)
			if err != nil {
				continue
			}
			events, err := rtcsignal.DecodeWebsocketMessage(data, m.Type == "receive")
			if err != nil {
				continue
			}
			for _, ev := range events {
				if _, ok := ev.Frame.(*dgw.DataFrame); !ok || ev.Err != nil || ev.Message == nil {
					continue
				}
				walkE2ee(reflect.ValueOf(ev.Message), m.Type, out)
			}
		}
	}
	return out
}

var (
	stateStoreType  = reflect.TypeOf(rtcsignal.StateStore{})
	dataMessageType = reflect.TypeOf(rtcsignal.DataMessage{})
)

func walkE2ee(v reflect.Value, dir string, out *harE2ee) {
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			walkE2ee(v.Elem(), dir, out)
		}
	case reflect.Struct:
		if v.Type() == dataMessageType {
			dm := v.Addr().Interface().(*rtcsignal.DataMessage)
			if dm.Topic == "E2eeKey" {
				out.Keys = append(out.Keys, harBlob{Dir: dir, Sender: dm.Sender, Data: dm.Data})
			}
			return
		}
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).IsExported() {
				walkE2ee(v.Field(i), dir, out)
			}
		}
	case reflect.Slice:
		if v.Type() == stateStoreType {
			for _, ts := range v.Interface().(rtcsignal.StateStore) {
				if ts.Topic == rtcsignal.TopicE2eeState {
					out.States = append(out.States, harBlob{Dir: dir, Data: ts.Data})
				}
			}
			return
		}
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return
		}
		for i := 0; i < v.Len(); i++ {
			walkE2ee(v.Index(i), dir, out)
		}
	}
}
