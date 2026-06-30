// pkg/aichatviewer/chunked_request.go
//
// Client-side compression + chunking for outbound NATS requests, mirroring
// `nats-service-client` (`pkg/nats-service-client/nats-service-client.go:144+`)
// so the API's existing `nats-service` endpoint handler (which already
// transparently downloads + decompresses chunked requests in
// `createNatsMessageFromRequest`) sees a normal-sized body.
//
// Why not call the library's Client.DoRequest? It's unary (synchronous
// request/response). Our chat path is multi-response streaming — the inbox
// receives N events from the agent. We replicate just the request-side
// compress+chunk protocol against our existing PublishMsg + inbox flow.
package webbridge

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service "github.com/transactrx/nats-service/pkg/nats-service"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"
)

// Thresholds match `nats-service-client` library defaults. NATS server default
// max_payload is 1 MB; chunkPayloadSize stays comfortably below that.
const (
	compressThreshold = 2 * 1024
	chunkPayloadSize  = 200 * 1024
)

// prepareLargeRequest applies the library's compress+chunk protocol to msg.
// If the body is small, msg is left as-is and cleanup is a no-op. If the body
// exceeds compressThreshold it is gzipped (with `_Compression: gzip` header).
// If the gzipped body exceeds chunkPayloadSize, msg.Data is cleared and the
// CHUNKED_SUBJECT / CHUNKED_LENGTH / CHUNKS_ID headers are set; a temporary
// subscription is created on the chunk subject and returned via cleanup, which
// the caller MUST invoke once the request lifecycle ends (streamSubscription
// terminates).
func prepareLargeRequest(nc *nats.Conn, msg *nats.Msg, logger *log.Logger) (func(), error) {
	data := msg.Data
	if len(data) > compressThreshold {
		data = nats_service_common.GZipBytes(data)
		msg.Header.Set(nats_service_common.COMPRESSED_HEADER, nats_service_common.GZIP_COMPRESSION_TYPE)
	}
	if len(data) <= chunkPayloadSize {
		msg.Data = data
		return func() {}, nil
	}

	chunks := nats_service_common.ChunkByteArray(data, chunkPayloadSize)
	chunksId := uuid.New().String()
	chunkSubject := nats.NewInbox()

	sub, err := nc.Subscribe(chunkSubject, func(req *nats.Msg) {
		idxStr := ""
		if req.Header != nil {
			idxStr = req.Header.Get(nats_service_common.CHUNK_INDEX)
		}
		idx, perr := strconv.Atoi(idxStr)
		if perr != nil || idx < 0 || idx >= len(chunks) {
			respondChunkError(req, idx, len(chunks))
			return
		}
		resp := &nats.Msg{
			Subject: req.Reply,
			Data:    chunks[idx],
			Header:  nats.Header{},
		}
		resp.Header.Set(nats_service_common.STATUS, "200")
		if rerr := req.RespondMsg(resp); rerr != nil && logger != nil {
			logger.Printf("aichatviewer: chunk respond failed for index %d: %v", idx, rerr)
		}
	})
	if err != nil {
		return nil, fmt.Errorf("aichatviewer: subscribe chunk subject: %w", err)
	}

	msg.Header.Set(nats_service_common.CHUNKED_SUBJECT, chunkSubject)
	msg.Header.Set(nats_service_common.CHUNKED_LENGTH, strconv.Itoa(len(chunks)))
	msg.Header.Set(nats_service_common.CHUNKS_ID, chunksId)
	msg.Data = nil

	if logger != nil {
		logger.Printf("aichatviewer: outbound request chunked into %d pieces of %d bytes (post-compress)", len(chunks), chunkPayloadSize)
	}

	return func() { _ = sub.Unsubscribe() }, nil
}

func respondChunkError(req *nats.Msg, idx, total int) {
	resp := &nats.Msg{Subject: req.Reply, Header: nats.Header{}}
	resp.Header.Set(nats_service_common.STATUS, "400")
	natsErr := nats_service.NatsServiceError{
		Status:       400,
		ErrorMessage: fmt.Sprintf("invalid chunk index %d (have %d)", idx, total),
	}
	resp.Data, _ = json.Marshal(natsErr)
	_ = req.RespondMsg(resp)
}
