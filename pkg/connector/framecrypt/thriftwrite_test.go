package framecrypt

import "sort"

// cw is a minimal Thrift compact-protocol writer for building test
// fixtures (the server's E2eeServerState), which rtcsignal only decodes.
type cw struct {
	b    []byte
	last []int16
}

const (
	ctTrue   = 1
	ctFalse  = 2
	ctI16    = 4
	ctI32    = 5
	ctI64    = 6
	ctBinary = 8
	ctList   = 9
	ctMap    = 11
	ctStruct = 12
)

func (w *cw) varint(v uint64) {
	for v >= 0x80 {
		w.b = append(w.b, byte(v)|0x80)
		v >>= 7
	}
	w.b = append(w.b, byte(v))
}
func (w *cw) zz(v int64) { w.varint(uint64(v<<1) ^ uint64(v>>63)) }

func (w *cw) begin() { w.last = append(w.last, 0) }
func (w *cw) end() {
	w.b = append(w.b, 0)
	w.last = w.last[:len(w.last)-1]
}
func (w *cw) field(id int16, t byte) {
	last := &w.last[len(w.last)-1]
	if d := id - *last; d > 0 && d <= 15 {
		w.b = append(w.b, byte(d)<<4|t)
	} else {
		w.b = append(w.b, t)
		w.zz(int64(id))
	}
	*last = id
}
func (w *cw) i16(id int16, v int16) { w.field(id, ctI16); w.zz(int64(v)) }
func (w *cw) i32(id int16, v int32) { w.field(id, ctI32); w.zz(int64(v)) }
func (w *cw) boolf(id int16, v bool) {
	if v {
		w.field(id, ctTrue)
	} else {
		w.field(id, ctFalse)
	}
}
func (w *cw) bin(id int16, v []byte) { w.field(id, ctBinary); w.binv(v) }
func (w *cw) binv(v []byte)          { w.varint(uint64(len(v))); w.b = append(w.b, v...) }
func (w *cw) listHeader(et byte, n int) {
	if n < 15 {
		w.b = append(w.b, byte(n)<<4|et)
	} else {
		w.b = append(w.b, 0xf0|et)
		w.varint(uint64(n))
	}
}
func (w *cw) structf(id int16, f func()) { w.field(id, ctStruct); w.begin(); f(); w.end() }

// testEndpoint is one entry of E2eeServerState.endpointInfos.
type testEndpoint struct {
	PreKeyBundle    []byte
	IdentityKeyMode int16
	DeviceID        int32
}

// buildServerState encodes an E2eeServerState in the shape of the one the
// server sent in the captured SFU call's first media update (field ids and
// config values as decoded from that capture; see har_test.go): suite 2,
// protocol version 7..7, keyNegotiationMode 1, the captured config
// timings, and endpointInfos keyed "<userId>:<cname>".
func buildServerState(endpoints map[string]testEndpoint) []byte {
	return buildServerStateWith(capturedConfig, endpoints)
}

// serverConfig is E2eeServerStateConfig (E2eeStateSerializers; field ids
// in comments).
type serverConfig struct {
	CipherSuite                         int16 // E2eeServerState.cipherSuites[0]
	KeyNegotiationMode                  int16 // 1
	RemoveFrameDecryptorDelayMs         int32 // 4
	KeepFrameDecryptors                 bool  // 5
	SenderKeyUpdateDelayMs              int32 // 6
	KeyTTLMs                            int32 // 7
	RatchetSpace                        int16 // 8
	KeyOverMediaDataChannelNumSends     int32 // 9
	KeyOverMediaDataChannelRetryDelayMs int32 // 10
	EnableH264V2                        bool  // 11
}

// capturedConfig is the config of the captured call's server states.
var capturedConfig = serverConfig{
	CipherSuite: 2, KeyNegotiationMode: 1, RemoveFrameDecryptorDelayMs: 15000,
	SenderKeyUpdateDelayMs: 10000, KeyTTLMs: 30000, RatchetSpace: 8,
	KeyOverMediaDataChannelNumSends: 70, KeyOverMediaDataChannelRetryDelayMs: 500,
	EnableH264V2: true,
}

func buildServerStateWith(cfg serverConfig, endpoints map[string]testEndpoint) []byte {
	w := &cw{}
	w.begin()
	w.field(2, ctList)
	w.listHeader(ctI16, 1)
	w.zz(int64(cfg.CipherSuite))
	w.structf(3, func() {
		w.field(1, ctList)
		w.listHeader(ctStruct, 1)
		w.begin()
		w.i16(1, 7)
		w.i16(2, 7)
		w.end()
	})
	w.structf(4, func() {
		w.i16(1, cfg.KeyNegotiationMode)
		w.i32(4, cfg.RemoveFrameDecryptorDelayMs)
		w.boolf(5, cfg.KeepFrameDecryptors)
		w.i32(6, cfg.SenderKeyUpdateDelayMs)
		w.i32(7, cfg.KeyTTLMs)
		w.i16(8, cfg.RatchetSpace)
		w.i32(9, cfg.KeyOverMediaDataChannelNumSends)
		w.i32(10, cfg.KeyOverMediaDataChannelRetryDelayMs)
		w.boolf(11, cfg.EnableH264V2)
		w.field(12, ctList)
		w.listHeader(ctI32, 0)
		w.boolf(13, false)
		w.i32(14, 0)
	})
	w.field(5, ctMap)
	keys := make([]string, 0, len(endpoints))
	for k := range endpoints {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	w.varint(uint64(len(keys)))
	if len(keys) > 0 {
		w.b = append(w.b, ctBinary<<4|ctStruct)
		for _, k := range keys {
			e := endpoints[k]
			w.binv([]byte(k))
			w.begin()
			w.bin(1, e.PreKeyBundle)
			w.structf(2, func() { w.i16(1, e.IdentityKeyMode) })
			w.i32(3, e.DeviceID)
			w.end()
		}
	}
	w.field(6, ctMap)
	w.varint(0)
	w.i32(7, 0)
	w.i32(9, 0)
	w.end()
	return w.b
}
