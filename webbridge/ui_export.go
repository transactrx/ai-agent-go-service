package webbridge

import (
	"io/fs"

	"github.com/transactrx/ai-agent-go-service/webbridge/ui"
)

// DefaultWebixUI is the embedded aichat-webix asset tree, re-exported from
// the ui sub-package. Pass to Options.UI to serve the built-in Webix chat UI.
var DefaultWebixUI fs.FS = ui.DefaultWebixUI

// NoUI is an explicit nil fs.FS sentinel; equivalent to leaving Options.UI unset.
var NoUI fs.FS = nil
