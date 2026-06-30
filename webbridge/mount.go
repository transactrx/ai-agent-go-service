package webbridge

import (
	"context"
	"errors"
	"io/fs"
	"log"
	"net/http"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/filesystem"
)

// S3Charts holds the S3 bucket config for the chart presign route.
type S3Charts struct {
	Bucket string
	Region string
}

// Options configures a Mount call. Auth and NATSChatPath are required; all
// other fields default gracefully when nil/zero.
type Options struct {
	// NATSChatPath is the NATS base subject (== chatApi NATS_BASE_PATH). Required.
	NATSChatPath string
	// Auth is the host's identity seam. Required.
	Auth Authenticator
	// Authorizer gates workflow access. nil => AllowAll.
	Authorizer Authorizer
	// Logger is the logger for bridge operations. nil => log.Default().
	Logger *log.Logger
	// UI is an optional fs.FS to serve at UIMountPath. nil => no UI route.
	// Pass DefaultWebixUI (Task 2.6) to serve the embedded Webix chat UI.
	UI fs.FS
	// UIMountPath is the prefix at which UI is mounted. Default "/aichatviewer/ui".
	UIMountPath string
	// Charts enables the presigned chart redirect route when non-nil.
	Charts *S3Charts
	// Uploads enables the attachment upload route when non-nil.
	Uploads AttachmentStorage
	// UploadConfig tunes the upload route. nil => defaultUploadConfig.
	UploadConfig *UploadConfig
}

// defaultUploadConfig is used when Options.UploadConfig is nil.
var defaultUploadConfig = UploadConfig{
	MaxBytes:    25 * 1024 * 1024, // 25 MB
	AllowedMime: []string{"image/*", "application/pdf", "text/csv", "text/plain", "application/json"},
}

// Mount registers the aichatviewer routes on router. Returns an error if
// required options are missing or chart presigner construction fails.
func Mount(router fiber.Router, o Options) error {
	if o.Auth == nil {
		return errors.New("webbridge: Options.Auth is required")
	}
	if o.NATSChatPath == "" {
		return errors.New("webbridge: Options.NATSChatPath is required")
	}
	lg := o.Logger
	if lg == nil {
		lg = log.Default()
	}
	authz := o.Authorizer
	if authz == nil {
		authz = AllowAll{}
	}
	b := &bridge{auth: o.Auth, authz: authz, natsBasePath: o.NATSChatPath, logger: lg}

	// Core routes — always registered.
	router.Post("/aichatviewer/token", b.issueToken)
	router.Get("/aichatviewer/workflows", b.listWorkflows)
	router.Use("/aichatviewer/stream", b.preUpgrade)
	router.Get("/aichatviewer/stream", websocket.New(b.streamChat))

	// Upload routes — optional.
	if o.Uploads != nil {
		cfg := defaultUploadConfig
		if o.UploadConfig != nil {
			cfg = *o.UploadConfig
		}
		router.Use("/aichatviewer/upload", b.requireAccount)
		router.Post("/aichatviewer/upload", UploadAttachment(o.Uploads, cfg))
		if local, ok := o.Uploads.(*LocalStorage); ok {
			router.Use("/aichatviewer/upload-local/:id/:filename", b.requireAccount)
			router.Get("/aichatviewer/upload-local/:id/:filename", uploadLocalGet(local))
		}
	}

	// Chart presign redirect — optional.
	if o.Charts != nil {
		presigner, err := newS3ChartPresigner(context.Background(), o.Charts.Bucket, o.Charts.Region)
		if err != nil {
			lg.Printf("webbridge: chart redirect disabled: %v", err)
		} else {
			router.Use("/aichatviewer/chart", b.requireAccount)
			router.Get("/aichatviewer/chart/:name", chartRedirectHandler(presigner))
			lg.Printf("webbridge: chart redirect enabled bucket=%s region=%s", o.Charts.Bucket, o.Charts.Region)
		}
	}

	// UI serving — optional.
	if o.UI != nil {
		mountPath := o.UIMountPath
		if mountPath == "" {
			mountPath = "/aichatviewer/ui"
		}
		router.Use(mountPath, filesystem.New(filesystem.Config{Root: http.FS(o.UI)}))
	}

	return nil
}

// requireAccount is middleware that authenticates and gates upload/chart routes.
// It resolves the caller identity via b.auth.Identify, sets c.Locals for
// downstream handlers, and returns 401 if identity is missing.
func (b *bridge) requireAccount(c *fiber.Ctx) error {
	id, err := b.auth.Identify(c)
	if err != nil || id.AccountID == "" {
		return c.SendStatus(http.StatusUnauthorized)
	}
	c.Locals(localAccountID, id.AccountID)
	c.Locals(localUserID, id.UserID)
	return c.Next()
}
