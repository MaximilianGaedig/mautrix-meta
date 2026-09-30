package connector

import (
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

// testMetaClient is a client of a made-up login, enough to make IDs and events without a network.
func testMetaClient() *MetaClient {
	return &MetaClient{
		Main:      &MetaConnector{Bridge: &bridgev2.Bridge{Config: &bridgeconfig.BridgeConfig{}}},
		UserLogin: &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "1000"}},
		LoginMeta: &metaid.UserLoginMetadata{Platform: types.Facebook},
	}
}
