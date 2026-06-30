package ui

import (
	"embed"
	"io/fs"
)

//go:embed all:assets/aichat-webix
var embedded embed.FS

// DefaultWebixUI is the embedded aichat-webix asset tree rooted so that
// "/aichatviewer/ui/<file>" maps to assets/aichat-webix/<file>.
var DefaultWebixUI fs.FS = mustSub(embedded, "assets/aichat-webix")

func mustSub(f embed.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}
