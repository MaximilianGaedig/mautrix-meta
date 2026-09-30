package msgconv

import "regexp"

// callLogAdminText matches the service messages Messenger writes into a chat about a call, in the English the
// bridge's sessions use: "You missed a video call from Anna.", "The video call ended.". They say what the
// bridge's own call log line says, so with the call log on they aren't bridged.
var callLogAdminText = regexp.MustCompile(`(?i)^\s*(you missed an? (video |audio |voice |group )?(call|chat)\b|the (video |audio |voice |group )?(call|chat) ended\b)`)

func isCallLogAdminText(text string) bool {
	return callLogAdminText.MatchString(text)
}
