// Package quickchart implements tool/quickchart — N8N-parity chart rendering.
// Posts a Chart.js config to https://quickchart.io and returns a hosted
// image URL. Includes a schema-level PHI scrub to refuse charts whose data
// shape references known patient-identifier fields.
package quickchart

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/transactrx/ai-agent-go-service/pkg/aws/s3files"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/node"
)

const (
	defaultToolName       = "QuickChart"
	defaultHost           = "https://quickchart.io"
	defaultWidth          = 600
	defaultHeight         = 400
	defaultBackground     = "white"
	defaultFormat         = "png"
	defaultRequestTimeout = 10
)

var defaultPhiScrubFields = []string{"firstName", "lastName", "memberId", "dob", "dateOfBirth", "ssn", "address", "phone", "email"}

// Config validated by Factory.
type Config struct {
	Host                  string   `json:"host,omitempty"`
	Width                 int      `json:"width,omitempty"`
	Height                int      `json:"height,omitempty"`
	BackgroundColor       string   `json:"backgroundColor,omitempty"`
	Format                string   `json:"format,omitempty"`
	RequestTimeoutSeconds int      `json:"requestTimeoutSeconds,omitempty"`
	FailurePolicyValue    string   `json:"failurePolicy,omitempty"`
	ToolName              string   `json:"toolName,omitempty"`
	ToolDescription       string   `json:"toolDescription"`
	PhiScrubFields        []string `json:"phiScrubFields,omitempty"`
	// ChartURLPrefix, when non-empty, makes the tool return a short relative
	// URL (prefix + "/" + key, e.g. "aichatviewer/chart/<uuid>.png") instead
	// of the long presigned S3 URL. The consuming webapp resolves it via a
	// redirect endpoint that re-signs on demand. Empty = legacy presigned URL.
	ChartURLPrefix string `json:"chartUrlPrefix,omitempty"`
}

var Factory node.Factory = node.FactoryFunc(func(rawConfig json.RawMessage) (node.Node, error) {
	var cfg Config
	if err := json.Unmarshal(rawConfig, &cfg); err != nil {
		return nil, fmt.Errorf("tool/quickchart: parse config: %w", err)
	}
	if cfg.ToolDescription == "" {
		return nil, fmt.Errorf("tool/quickchart: toolDescription is required")
	}
	if cfg.Host == "" {
		cfg.Host = defaultHost
	}
	if cfg.Width == 0 {
		cfg.Width = defaultWidth
	}
	if cfg.Height == 0 {
		cfg.Height = defaultHeight
	}
	if cfg.BackgroundColor == "" {
		cfg.BackgroundColor = defaultBackground
	}
	if cfg.Format == "" {
		cfg.Format = defaultFormat
	}
	if cfg.RequestTimeoutSeconds == 0 {
		cfg.RequestTimeoutSeconds = defaultRequestTimeout
	}
	if cfg.FailurePolicyValue == "" {
		cfg.FailurePolicyValue = string(node.FailureSurfaceToLLM)
	}
	if cfg.ToolName == "" {
		cfg.ToolName = defaultToolName
	}
	if cfg.PhiScrubFields == nil {
		cfg.PhiScrubFields = append([]string{}, defaultPhiScrubFields...)
	}
	return &quickchartTool{cfg: cfg}, nil
})

type quickchartTool struct {
	node.BaseTool
	cfg      Config
	http     *http.Client
	logger   *log.Logger
	nodeID   string
	wfID     string
	uploader s3files.Uploader
}

func (t *quickchartTool) Spec() node.NodeSpec {
	return node.NodeSpec{
		Type: "tool/quickchart",
		Role: node.RoleTool,
		OutputPorts: []node.PortSpec{
			{Name: node.PortAITool, Direction: node.PortOut, Cardinality: node.CardOne},
		},
	}
}

func (t *quickchartTool) FailurePolicy() node.FailurePolicy {
	return node.FailurePolicy(t.cfg.FailurePolicyValue)
}

func (t *quickchartTool) Init(_ context.Context, env node.NodeEnv) error {
	t.InitRetry(env)
	t.http = &http.Client{Timeout: time.Duration(t.cfg.RequestTimeoutSeconds) * time.Second}
	t.logger = env.Logger()
	t.nodeID = env.NodeID()
	t.wfID = env.WorkflowID()
	host, ok := env.Host("s3files")
	if !ok {
		return fmt.Errorf("tool/quickchart: host \"s3files\" is required (boot the binary with an S3 uploader)")
	}
	up, ok := host.(s3files.Uploader)
	if !ok {
		return fmt.Errorf("tool/quickchart: host \"s3files\" has wrong type %T", host)
	}
	t.uploader = up
	return nil
}

func (t *quickchartTool) Close(_ context.Context) error { return nil }

func (t *quickchartTool) ToolSpec() node.ToolSpec {
	return node.ToolSpec{
		Name:        t.cfg.ToolName,
		Description: t.cfg.ToolDescription,
		InputSchema: json.RawMessage(`{
		  "type": "object",
		  "required": ["type", "data"],
		  "additionalProperties": false,
		  "properties": {
		    "type":    {"enum": ["bar","line","pie","doughnut","scatter","horizontalBar"]},
		    "data":    {"type": "object"},
		    "options": {"type": "object"}
		  }
		}`),
	}
}
