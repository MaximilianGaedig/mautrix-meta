package framecrypt

import "go.mau.fi/mautrix-meta/pkg/connector/framecrypt/fcsim"

type (
	testEndpoint = fcsim.Endpoint
	serverConfig = fcsim.Config
)

var capturedConfig = fcsim.CapturedConfig

// buildServerState encodes an E2eeServerState in the shape of the one the
// server sent in the captured SFU call's first media update (field ids and
// config values as decoded from that capture; see har_test.go): suite 2,
// protocol version 7..7, keyNegotiationMode 1, the captured config
// timings, and endpointInfos keyed "<userId>:<cname>".
func buildServerState(endpoints map[string]testEndpoint) []byte {
	return fcsim.ServerState(capturedConfig, endpoints)
}

func buildServerStateWith(cfg serverConfig, endpoints map[string]testEndpoint) []byte {
	return fcsim.ServerState(cfg, endpoints)
}
