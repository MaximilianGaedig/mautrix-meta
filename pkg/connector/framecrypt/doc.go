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

// Package framecrypt runs Messenger's frame-encryption WebAssembly module
// (the emscripten build the web client loads as "frame_encryption") in
// process with wazero, so the bridge can take part in end-to-end encrypted
// group calls: publish the E2eeClientState, process the server's
// E2eeServerState and the peers' "E2eeKey" data messages, emit its own, and
// SFrame-encrypt/decrypt media frames. The API mirrors the web client's glue
// module FrameEncryptionWasm and its users ZenonE2eeCore,
// ZenonEncryptionKeysManager and ZenonSecureFrameManager.
//
// # Where the module comes from
//
// The module is not bundled; the bridge fetches it from Meta like the
// browser does and passes the bytes to NewRuntime. In the web client,
// FrameEncryptionWasm builds the emscripten Module with
//
//	locateFile: function() { return bx.getURL(bx("37")) }
//
// and bx (the static-resource map) is filled from "bxData" in the
// HasteSupportData payload that the group-call page
// (https://www.facebook.com/groupcall/ROOM:<id>/?call_id=...) embeds in a
// <script data-sjs> require block:
//
//	"bxData":{"37":{"uri":"https:\/\/static.xx.fbcdn.net\/rsrc.php\/yV\/r\/Wfd6t0e74Jr.wasm"}}
//
// The rsrc path is content-addressed and changes with client releases, and
// the page's bxData lists several .wasm resources (four in the captured
// pages), so pick the entry by key, not by extension. The key "37" is the
// build-time resource id FrameEncryptionWasm hard-codes (the bx("37") call
// in its __d("FrameEncryptionWasm", ...) definition, shipped in the
// rsrc.php JS bundles); a robust resolver reads the id from that call and
// then looks it up in bxData. The glue fetches the file with a plain GET
// (Referer https://www.facebook.com/, no credentials needed; streaming
// instantiate, 15 s init timeout) and it is served as application/wasm.
// NewRuntime checks that the exports this package calls are present, which
// catches picking the wrong resource.
//
// # How it is hosted
//
// The emscripten glue imports its memory ("env"."memory", a shared
// WebAssembly.Memory from WebAssemblyMemorySingleton) and installs JS
// callbacks with addFunction, which appends to the module's function
// table. A Go host cannot put host functions into a table directly, so
// rewriteModule turns the memory import into a defined memory and enlarges
// the table, and a small generated helper module fills the extra slots with
// host functions through an element segment. The remaining imports
// (embind registrations, the emscripten FS syscalls, time and WASI stdio)
// are implemented minimally in host.go.
package framecrypt
