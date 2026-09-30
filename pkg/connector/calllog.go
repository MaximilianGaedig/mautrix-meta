package connector

import (
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/calllog"
)

// callLogReports turns a call notice into what the call log shows: a line when the call starts, edited into its
// outcome when it ends. A call whose start the bridge didn't see (it was down, or Messenger only reported the
// end) gets its line when it ends, so the outcome is never lost.
//
// Messenger reports an answered call by its duration when it ends, so a call is never shown as in progress.
func callLogReports(log *calllog.Log, n *callNotice, sender bridgev2.EventSender) []bridgev2.RemoteEvent {
	callID := n.Key
	var reports []bridgev2.RemoteEvent
	add := func(evt bridgev2.RemoteEvent) {
		if evt != nil {
			reports = append(reports, evt)
		}
	}
	start := func() {
		add(log.Start(callID, calllog.Call{
			Portal:   n.Portal,
			Caller:   sender,
			Video:    n.Video,
			Outgoing: sender.IsFromMe || n.Kind == noticeOutgoing || n.Kind == noticeNotAnswered,
			Started:  n.Time,
		}))
	}
	switch n.Kind {
	case noticeIncoming, noticeOutgoing, noticeOngoing:
		start()
	case noticeMissed, noticeNotAnswered:
		start()
		add(log.End(callID, n.Time))
	default:
		start()
		answeredAt := n.Time
		if n.HasDuration && n.Duration > 0 {
			answeredAt = n.Time.Add(-n.Duration)
		}
		// The answer only makes the edit to the final line say how long the call was.
		log.Answer(callID, answeredAt)
		add(log.End(callID, n.Time))
	}
	return reports
}
