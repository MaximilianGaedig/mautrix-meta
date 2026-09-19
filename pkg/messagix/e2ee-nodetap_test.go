package messagix

import (
	"testing"

	waBinary "go.mau.fi/whatsmeow/binary"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestNodeTapLogger(t *testing.T) {
	var got []*waBinary.Node
	log := wrapE2EELogger(waLog.Noop, func(n *waBinary.Node) { got = append(got, n) })
	node := &waBinary.Node{Tag: "notification", Attrs: waBinary.Attrs{"type": "fb:call"}}
	// whatsmeow: recvLog := log.Sub("Recv"); recvLog.Debugf("%s", node)
	log.Sub("Recv").Debugf("%s", node)
	log.Sub("Send").Debugf("%s", node)
	log.Debugf("%s", node)
	log.Sub("Client").Sub("Recv").Debugf("%s", node)
	if len(got) != 2 || got[0] != node {
		t.Fatalf("tap saw %d nodes, want 2", len(got))
	}
	if wrapE2EELogger(waLog.Noop, nil) != waLog.Noop {
		t.Fatal("nil tap should not wrap")
	}
}
