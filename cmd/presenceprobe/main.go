// presenceprobe asks Messenger for people's presence the way the bridge does - over the
// PresenceUnifiedJSON stream, the only place Messenger answers it - and prints every publish.
//
// It reads a login's metadata (the bridge's user_login.metadata: cookies and platform) on stdin:
//
//	psql -tAc "select metadata from user_login" | presenceprobe [-raw] [-for 60s] <messenger id>...
//
// With ids, only their updates are printed and they are asked about like the bridge asks about
// 1:1 partners; without, the whole friend list is.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

type loginMetadata struct {
	Platform types.Platform   `json:"platform"`
	Cookies  *cookies.Cookies `json:"cookies"`
}

type line struct {
	At         string `json:"at"`
	Type       string `json:"publish"`
	UserID     int64  `json:"user_id"`
	Active     bool   `json:"active"`
	Status     int    `json:"presence_status"`
	LastActive string `json:"last_active,omitempty"`
	Caps       int64  `json:"capabilities,omitempty"`
}

func main() {
	raw := flag.Bool("raw", false, "also print each publish exactly as Messenger sent it")
	duration := flag.Duration("for", 60*time.Second, "how long to listen")
	verbose := flag.Bool("v", false, "log the connection")
	limit := flag.Int("max", messagix.MaxPresenceContacts, "most ids to ask about in one request")
	flag.Parse()
	messagix.MaxPresenceContacts = *limit

	var ids []int64
	wanted := map[int64]bool{}
	args := flag.Args()
	if len(args) == 1 && args[0] == "-" {
		// Ids on a file descriptor 3, one per line, for more than a command line holds.
		data, err := io.ReadAll(os.NewFile(3, "ids"))
		if err != nil {
			fail("reading ids from fd 3: %v", err)
		}
		args = strings.Fields(string(data))
	}
	for _, arg := range args {
		id, err := strconv.ParseInt(arg, 10, 64)
		if err != nil {
			fail("not a Messenger id: %s", arg)
		}
		ids = append(ids, id)
		wanted[id] = true
	}

	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		fail("reading stdin: %v", err)
	}
	var meta loginMetadata
	if err = json.Unmarshal(data, &meta); err != nil || meta.Cookies == nil {
		fail("stdin is not a login's metadata: %v", err)
	}
	meta.Cookies.Platform = meta.Platform

	// The page decoder warns about every table it doesn't know, through the global logger too.
	level := zerolog.ErrorLevel
	if *verbose {
		level = zerolog.DebugLevel
	}
	zerolog.SetGlobalLevel(level)
	log := zerolog.New(zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) { w.Out = os.Stderr })).
		Level(level).With().Timestamp().Logger()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	cli := messagix.NewClient(meta.Cookies, log, &messagix.Config{})
	out := json.NewEncoder(os.Stdout)
	cli.SetEventHandler(func(_ context.Context, evt any) {
		pub, ok := evt.(*messagix.PresenceEvent)
		if !ok {
			return
		}
		kind := "change"
		if pub.IsFull() {
			kind = "snapshot"
		}
		if *raw {
			fmt.Fprintf(os.Stdout, "raw %s\n", pub.Raw)
		}
		now := time.Now().Format(time.RFC3339)
		for _, u := range pub.PresenceUpdates {
			if len(wanted) > 0 && !wanted[int64(u.UserID)] {
				continue
			}
			l := line{At: now, Type: kind, UserID: int64(u.UserID), Active: u.IsActive(),
				Status: int(u.PresenceStatus), Caps: int64(u.Capabilities)}
			if t := u.LastActive(); !t.IsZero() {
				l.LastActive = t.Format(time.RFC3339)
			}
			_ = out.Encode(l)
		}
	})

	if _, _, err = cli.LoadMessagesPage(ctx); err != nil {
		fail("loading Messenger with the stored session: %v", err)
	}
	if len(ids) > 0 {
		cli.SetPresenceContacts(ids)
	}
	if err = cli.StartPresenceStream(ctx); err != nil {
		fail("opening the presence stream: %v", err)
	}
	<-ctx.Done()
	cli.StopPresenceStream()
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "presenceprobe: "+format+"\n", args...)
	os.Exit(1)
}
