package engine

import "time"

// MetricsRecorder is the cycle-1 seam for future Prometheus/CloudWatch
// implementations. Cycle 1 wires NoOpMetrics; cycle 2+ swaps in concrete impls.
type MetricsRecorder interface {
	RequestStarted(workflowID string)
	RequestEnded(workflowID, code string, elapsed time.Duration)
	NodeInvoked(nodeID, nodeType string, elapsed time.Duration)
	StreamEventSent(workflowID string, eventType string)
}

// NoOpMetrics is the cycle-1 default.
type NoOpMetrics struct{}

func (NoOpMetrics) RequestStarted(string)                          {}
func (NoOpMetrics) RequestEnded(string, string, time.Duration)     {}
func (NoOpMetrics) NodeInvoked(string, string, time.Duration)      {}
func (NoOpMetrics) StreamEventSent(string, string)                 {}
