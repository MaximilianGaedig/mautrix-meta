#!/bin/sh
# Watchtower pre-update hook: put an update off while a call is being bridged, because restarting
# the bridge drops every call. Watchtower skips the update when this exits with 75 (EX_TEMPFAIL)
# and tries again at its next poll.
#
# Label the container with
#   com.centurylinklabs.watchtower.lifecycle.pre-update=/docker-pre-update.sh
# and run watchtower with --enable-lifecycle-hooks (WATCHTOWER_LIFECYCLE_HOOKS=true).
#
# Fails open: if the bridge doesn't answer (not started, crashed, an older build), the update goes
# ahead, so a broken bridge can still be replaced by a fixed one.

url=$CALL_ACTIVE_URL
if [ -z "$url" ]; then
	port=$(yq e '.appservice.port' "${CONFIG_PATH:-/data/config.yaml}" 2>/dev/null)
	case "$port" in
	'' | *[!0-9]*) port=29319 ;;
	esac
	url="http://127.0.0.1:$port/_matrix/mau/call_active"
fi

if command -v curl >/dev/null 2>&1; then
	body=$(curl -fsS --max-time 5 "$url" 2>/dev/null) || exit 0
else
	body=$(wget -q -T 5 -O - "$url" 2>/dev/null) || exit 0
fi

case "$body" in
*'"active":true'*)
	echo "A call is in progress, not updating now: $body" >&2
	exit 75
	;;
esac
exit 0
