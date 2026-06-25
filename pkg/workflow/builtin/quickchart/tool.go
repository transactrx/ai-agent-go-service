package quickchart

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const (
	// chartReadCap caps the response body we read from QuickChart. Above this
	// either the chart is enormous (rare) or the server is misbehaving.
	chartReadCap = 8 * 1024 * 1024 // 8 MiB
	// chartMinBytes is the smallest body we accept as a real PNG. Real PNGs
	// for our chart sizes are several KiB; under 200 bytes is always wrong.
	chartMinBytes = 200
	// chartLogHead is how much of an error body we splash into logs/errors.
	chartLogHead = 500
)

func (t *quickchartTool) Invoke(ctx context.Context, args json.RawMessage) (json.RawMessage, error) {
	var in struct {
		Type    string          `json:"type"`
		Data    json.RawMessage `json:"data"`
		Options json.RawMessage `json:"options,omitempty"`
	}
	if err := json.Unmarshal(args, &in); err != nil {
		return nil, fmt.Errorf("tool/quickchart: bad input: %w", err)
	}
	if in.Type == "" {
		return nil, fmt.Errorf("tool/quickchart: type is required")
	}
	if len(in.Data) == 0 {
		return nil, fmt.Errorf("tool/quickchart: data is required")
	}
	t.logger.Printf("chart-trace: quickchart invoke type=%q dataBytes=%d optionsBytes=%d", in.Type, len(in.Data), len(in.Options))

	hit, err := ScanForPHI(in.Data, t.cfg.PhiScrubFields)
	if err != nil {
		return nil, fmt.Errorf("tool/quickchart: scrub failed: %w", err)
	}
	if hit != "" {
		return nil, fmt.Errorf("tool/quickchart: chart input contains PHI field name %q; aggregate or anonymize first", hit)
	}

	enrichedData, err := injectDefaultPalette(in.Data)
	if err != nil {
		return nil, fmt.Errorf("tool/quickchart: enrich data: %w", err)
	}

	body := map[string]any{
		"chart": map[string]any{
			"type": in.Type,
			"data": json.RawMessage(enrichedData),
		},
		"width":           t.cfg.Width,
		"height":          t.cfg.Height,
		"backgroundColor": t.cfg.BackgroundColor,
		"format":          t.cfg.Format,
	}
	if len(in.Options) > 0 {
		body["chart"].(map[string]any)["options"] = json.RawMessage(in.Options)
	}
	bodyBytes, _ := json.Marshal(body)

	endpoint := t.cfg.Host + "/chart"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "image/png")

	resp, err := t.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("tool/quickchart: request: %w", err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, chartReadCap+1))
	if err != nil {
		return nil, fmt.Errorf("tool/quickchart: read response: %w", err)
	}

	if resp.StatusCode/100 != 2 {
		ct := resp.Header.Get("Content-Type")
		// Full body (even if binary) goes to logs for debugging...
		t.logger.Printf("tool/quickchart: render failed status=%d content-type=%q body=%q", resp.StatusCode, ct, truncate(respBody, chartLogHead))
		// ...but the model-facing error must stay clean. QuickChart renders
		// config errors as a PNG (format=png), so dumping the raw image bytes
		// into the tool error gives the model unreadable garbage it can't
		// recover from. Surface the body only when it is actually text.
		return nil, fmt.Errorf("tool/quickchart: chart render failed (status %d): %s", resp.StatusCode, errorBodyDetail(ct, respBody))
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "image/") {
		t.logger.Printf("tool/quickchart: non-image response content-type=%q body=%q", ct, truncate(respBody, chartLogHead))
		return nil, fmt.Errorf("tool/quickchart: chart render did not return an image (content-type=%q); the chart config may be invalid", ct)
	}
	if len(respBody) < chartMinBytes {
		return nil, fmt.Errorf("tool/quickchart: chart render returned a suspiciously small payload (%d bytes); the chart config may be invalid", len(respBody))
	}
	if len(respBody) > chartReadCap {
		return nil, fmt.Errorf("tool/quickchart: chart render exceeded %d byte cap", chartReadCap)
	}

	key, url, err := t.uploader.UploadChart(ctx, respBody)
	if err != nil {
		return nil, fmt.Errorf("tool/quickchart: upload chart: %w", err)
	}
	t.logger.Printf("tool/quickchart: rendered %d bytes -> %s", len(respBody), key)
	t.logger.Printf("chart-trace: quickchart uploaded key=%s urlLen=%d", key, len(url))

	imageURL := chartImageURL(t.cfg.ChartURLPrefix, key, url)
	t.logger.Printf("chart-trace: quickchart imageUrl prefix=%q -> %q", t.cfg.ChartURLPrefix, imageURL)
	return json.Marshal(map[string]string{
		"imageUrl": imageURL,
		"type":     in.Type,
	})
}

// chartImageURL decides what URL the model embeds for a rendered chart. When
// prefix is set, it returns a short, stable relative URL (prefix + "/" + key)
// the webapp redirect endpoint re-signs on demand; otherwise it returns the
// presigned S3 URL (legacy behavior). key is the S3 object key, e.g.
// "chart/<uuid>.png".
func chartImageURL(prefix, key, presigned string) string {
	if strings.TrimSpace(prefix) == "" {
		return presigned
	}
	return strings.TrimRight(prefix, "/") + "/" + key
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// errorBodyDetail produces a model-readable explanation of a QuickChart error
// response. QuickChart returns config errors as a rendered PNG (format=png), so
// a binary/image body is useless to the model — and worse, it derails the agent
// loop. When the body is text (a JSON/plain error) we pass it through;
// otherwise we emit actionable guidance so the model can fix and retry.
func errorBodyDetail(contentType string, body []byte) string {
	if isTextual(contentType) && len(body) > 0 {
		return truncate(body, chartLogHead)
	}
	return "QuickChart could not render the chart - this almost always means the Chart.js config is invalid. " +
		"Check that data.datasets is a non-empty array of objects like {\"label\":\"...\",\"data\":[numbers]}, " +
		"that every dataset's data length matches data.labels length, and that type is one of " +
		"bar, line, pie, doughnut, scatter, radar, polarArea. Fix the config and call QuickChart again."
}

// isTextual reports whether a Content-Type is safe to echo back to the model as
// text (vs. binary like image/* that must not be interpolated into an error).
func isTextual(contentType string) bool {
	ct := strings.ToLower(contentType)
	return strings.HasPrefix(ct, "text/") ||
		strings.Contains(ct, "json") ||
		strings.Contains(ct, "xml") ||
		strings.Contains(ct, "html")
}

// defaultPalette mirrors the N8N reference workflow's dataset color palette.
// Applied to datasets that omit backgroundColor.
var defaultPalette = []string{"#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F", "#EDC948", "#B07AA1", "#FF9DA7"}

// injectDefaultPalette walks data.datasets and adds the default palette to
// any dataset that doesn't already specify backgroundColor. Preserves all
// other fields (data, label, borderColor, etc.) verbatim.
func injectDefaultPalette(data json.RawMessage) (json.RawMessage, error) {
	var asMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &asMap); err != nil {
		return nil, err
	}
	datasetsRaw, ok := asMap["datasets"]
	if !ok {
		return data, nil // no datasets, nothing to enrich
	}
	var datasets []map[string]json.RawMessage
	if err := json.Unmarshal(datasetsRaw, &datasets); err != nil {
		return nil, err
	}
	changed := false
	for i, ds := range datasets {
		if _, has := ds["backgroundColor"]; has {
			continue
		}
		palette, err := json.Marshal(defaultPalette)
		if err != nil {
			return nil, err
		}
		ds["backgroundColor"] = palette
		datasets[i] = ds
		changed = true
	}
	if !changed {
		return data, nil
	}
	newDatasets, err := json.Marshal(datasets)
	if err != nil {
		return nil, err
	}
	asMap["datasets"] = newDatasets
	return json.Marshal(asMap)
}
