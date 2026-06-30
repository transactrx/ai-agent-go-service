// Package natsstream implements the canonical NATS multi-response streaming
// pattern used by workflow triggers in cycle 1. The pattern is: handler
// publishes events on the request's reply inbox via raw *nats.Conn and returns
// Status:302 so nats-service skips its auto-response.
//
// Wire protocol (spec §5.1):
//   _Stream_Event           start | delta | thought | tool_call | tool_result | complete | error
//   _Stream_Sequence        monotonically increasing int starting at 0
//   _Stream_Id              UUID; correlates events of one stream
//   _Stream_Cancel_Subject  set on first event (start) only
//   _Message_Id             same as _Stream_Id (lib convention)
//   status                  200 for data; 5xx on error terminator
//
// Sequence rule: 0 = start, 1..N-2 = data events in order, N-1 = terminator.
//
// Future migration: when nats-service ships AddStreamingEndpoint (spec §5.9),
// this package becomes a one-line wrapper or is deleted entirely. Wire format
// is preserved verbatim.
package natsstream
