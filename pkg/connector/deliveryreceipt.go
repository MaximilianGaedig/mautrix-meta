package connector

import (
	"sync"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/simplevent"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// Messenger says a message reached the other person's device the way it says
// one was read: not per message, but as a time up to which everything has
// been (LSUpdateDeliveryReceipt). The bridge reports delivery per message, so
// the time is turned into the messages it newly covers: those after the last
// such time this chat was given.
//
// That last time is only kept while the bridge runs. The first receipt after
// a start looks this far back instead, which is enough for a message sent
// shortly before a restart and does not report a chat's whole past as
// delivered again, one event per message.
const deliveryReceiptLookback = time.Hour

type deliveryWatermarks struct {
	lock sync.Mutex
	upTo map[networkid.PortalKey]time.Time
}

// advance records that a chat's messages up to a time were delivered, and
// returns the time after which that is news. It is not news at all when the
// chat was already known to be delivered that far.
func (w *deliveryWatermarks) advance(portal networkid.PortalKey, upTo time.Time) (after time.Time, news bool) {
	w.lock.Lock()
	defer w.lock.Unlock()
	previous, known := w.upTo[portal]
	if known && !upTo.After(previous) {
		return time.Time{}, false
	}
	if w.upTo == nil {
		w.upTo = make(map[networkid.PortalKey]time.Time)
	}
	w.upTo[portal] = upTo
	if known {
		return previous, true
	}
	return upTo.Add(-deliveryReceiptLookback), true
}

// deliveryTargets names each message once, however many parts it was bridged
// as. Whose messages they are is not looked at here: the bridge reports
// delivery only for those sent from Matrix.
func deliveryTargets(parts []*database.Message) []networkid.MessageID {
	targets := make([]networkid.MessageID, 0, len(parts))
	seen := make(map[networkid.MessageID]struct{}, len(parts))
	for _, part := range parts {
		if _, dup := seen[part.ID]; dup {
			continue
		}
		seen[part.ID] = struct{}{}
		targets = append(targets, part.ID)
	}
	return targets
}

func (m *MetaClient) handleUpdateDeliveryReceipt(tk handlerParams, msg *table.LSUpdateDeliveryReceipt) bridgev2.RemoteEvent {
	if msg.DeliveredWatermarkTimestampMs <= 0 {
		return nil
	}
	upTo := time.UnixMilli(msg.DeliveredWatermarkTimestampMs)
	after, news := m.deliveredUpTo.advance(tk.Portal, upTo)
	if !news {
		return nil
	}
	parts, err := m.Main.Bridge.DB.Message.GetMessagesBetweenTimeQuery(tk.ctx, tk.Portal, after, upTo)
	if err != nil {
		zerolog.Ctx(tk.ctx).Err(err).
			Int64("delivered_up_to", msg.DeliveredWatermarkTimestampMs).
			Msg("Failed to get the messages a delivery receipt covers")
		return nil
	}
	targets := deliveryTargets(parts)
	if len(targets) == 0 {
		return nil
	}
	return &simplevent.Receipt{
		EventMeta: simplevent.EventMeta{
			Type: bridgev2.RemoteEventDeliveryReceipt,
			LogContext: func(c zerolog.Context) zerolog.Context {
				return c.Int64("delivered_up_to", msg.DeliveredWatermarkTimestampMs)
			},
			PortalKey:         tk.Portal,
			UncertainReceiver: tk.IsUncertainReceiver(),
			Sender:            m.makeEventSender(msg.ContactId),
		},
		Targets: targets,
	}
}
