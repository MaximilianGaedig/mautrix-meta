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
	waBinary "go.mau.fi/whatsmeow/binary"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// nodeTapLogger wraps the whatsmeow logger to observe received nodes.
//
// whatsmeow has no public hook for notification types it doesn't handle (for
// example Messenger's <notification type="fb:call">, which it only logs as
// "Unhandled notification"). Every received node is, however, passed as a
// *waBinary.Node argument to the "Recv" sublogger's Debugf before dispatch,
// regardless of the log level, so wrapping the logger gives typed access to
// the node without forking whatsmeow. The tap runs on the websocket read
// loop and must not block.
type nodeTapLogger struct {
	waLog.Logger
	tap  func(*waBinary.Node)
	recv bool
}

func (l *nodeTapLogger) Debugf(msg string, args ...any) {
	if l.recv {
		for _, arg := range args {
			if node, ok := arg.(*waBinary.Node); ok {
				l.tap(node)
			}
		}
	}
	l.Logger.Debugf(msg, args...)
}

func (l *nodeTapLogger) Sub(module string) waLog.Logger {
	return &nodeTapLogger{Logger: l.Logger.Sub(module), tap: l.tap, recv: module == "Recv"}
}

func wrapE2EELogger(log waLog.Logger, tap func(*waBinary.Node)) waLog.Logger {
	if tap == nil {
		return log
	}
	return &nodeTapLogger{Logger: log, tap: tap}
}
