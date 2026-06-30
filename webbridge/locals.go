package webbridge

import "log"

// Locals keys shared between the upload/chart gates and the bridge handlers.
const (
	localAccountID = "aichat:accountId"
	localUserID    = "aichat:userId"
	localUserName  = "aichat:userName"
	localTimeZone  = "aichat:timeZone"
	localSessionID = "aichat:sessionId"
)

// logger is the package-level logger shared by upload/chart handlers and the
// bridge. Initialised to log.Default() so it works without explicit wiring.
// Task 2.3 (bridge.go) will expose a setter so callers can inject their own.
var logger = log.Default()
