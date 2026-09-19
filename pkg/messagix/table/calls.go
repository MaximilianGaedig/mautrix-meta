package table

// Lightspeed stored procedures that track ongoing RTC calls on threads.
//
// Their argument layouts are NOT known from the web client's JS (the SP
// modules were not in any capture). What is known: the web client keeps a
// threads.ongoingCallState column (LSRtcCallState) and a
// rtc_ongoing_calls_on_threads_v2 table keyed by threadKey with an
// ongoingCallServerInfoData column (the same base64 call handle as the E2EE
// fb:call notification's server_info_data). ThreadKey at index 0 follows the
// convention of every other thread-scoped SP and is an assumption; everything
// else is kept in Unrecognized, and consumers must validate values (for
// example with rtcsignal.ParseServerInfoData) before trusting them.

// LSRtcCallState mirrors the web client's LSRtcCallState enum.
type LSRtcCallState int64

const (
	RtcCallStateNone         LSRtcCallState = 0
	RtcCallStateAudioGroup   LSRtcCallState = 1
	RtcCallStateVideoGroup   LSRtcCallState = 2
	RtcCallStateVideo1to1    LSRtcCallState = 3
	RtcCallStateAudio1to1    LSRtcCallState = 4
	rtcCallStateMaxKnownEnum LSRtcCallState = 4
)

func (s LSRtcCallState) Valid() bool { return s >= 0 && s <= rtcCallStateMaxKnownEnum }
func (s LSRtcCallState) IsVideo() bool {
	return s == RtcCallStateVideoGroup || s == RtcCallStateVideo1to1
}

// LSUpdateThreadOngoingCallState sets threads.ongoingCallState.
// Index 1 (the new state) is inferred.
type LSUpdateThreadOngoingCallState struct {
	ThreadKey        int64          `index:"0" json:",omitempty"`
	OngoingCallState LSRtcCallState `index:"1" json:",omitempty"`

	Unrecognized map[int]any `json:",omitempty"`
}

func (ls *LSUpdateThreadOngoingCallState) GetThreadKey() int64 { return ls.ThreadKey }

// LSUpdateOrInsertRtcOngoingCallData upserts rtc_ongoing_calls_on_threads_v2.
// The server info data position is unknown; scan Unrecognized for it.
type LSUpdateOrInsertRtcOngoingCallData struct {
	ThreadKey int64 `index:"0" json:",omitempty"`

	Unrecognized map[int]any `json:",omitempty"`
}

func (ls *LSUpdateOrInsertRtcOngoingCallData) GetThreadKey() int64 { return ls.ThreadKey }

// LSDeleteRtcOngoingCallData deletes the rtc_ongoing_calls_on_threads_v2 row.
type LSDeleteRtcOngoingCallData struct {
	ThreadKey int64 `index:"0" json:",omitempty"`

	Unrecognized map[int]any `json:",omitempty"`
}

func (ls *LSDeleteRtcOngoingCallData) GetThreadKey() int64 { return ls.ThreadKey }
