package nats_service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"
	"log"
	"runtime/debug"
	"strconv"
	"time"
)

func (ns *NatService) startChunkResponder() error {

	subject := ns.basePath + "_.chunked_" + uuid.New().String()
	subscribe, err := ns.nc.Subscribe(subject, ns.handleChunkDataRequest)
	if err != nil {
		return err
	}
	if !subscribe.IsValid() {
		return fmt.Errorf("invalid subscription, possibly due to permissions")
	}
	ns.chunkedSubscription = subscribe
	go ns.cleanCacheService()
	return nil
}

func (ns *NatService) cleanCacheService() {

	defer func() {
		if r := recover(); r != nil {
			log.Printf("ERROR: cleanCacheService experienced a fatal error: %s. Stack trace\n%s\n", r, debug.Stack())
		}
	}()

	for {
		if ns.chunkCache.Len() > 0 {
			log.Printf("cleanCacheService: cleaning cache")
			log.Printf("cleanCacheService: cache size: %d", ns.chunkCache.Len())
			ns.chunkCache.DeleteExpired()
			log.Printf("cleanCacheService: cache cleaned")
			log.Printf("cleanCacheService: cache size: %d", ns.chunkCache.Len())
		}
		if !ns.chunkedSubscription.IsValid() {
			log.Print("cleanCacheService: chunkedSubscription is not valid, exiting")
		} else {
			if ns.debug {
				log.Print("cleanCacheService: chunkedSubscription is valid")
			}
		}
		time.Sleep(60 * time.Second)
	}
}

// chunk request handler
func (ns *NatService) handleChunkDataRequest(chunkReq *nats.Msg) {

	responseMsg := nats.Msg{Header: nats.Header{}}
	chunksId, chunkIndex, logger, err := ns.validateAndGetChunkRequestInfo(chunkReq)
	if err != nil {
		log.Printf("Unable to validate chunk request: %s", err)
		return
	}

	logger.Printf("handleChunkDataRequest: chunksId: %s, chunkIndex: %d", chunksId, chunkIndex)

	responseMsg.Data = ns.chunkCache.Get(chunksId).Value()[chunkIndex]
	responseMsg.Header.Set(nats_service_common.STATUS, "200")
	err = chunkReq.RespondMsg(&responseMsg)
	if err != nil {
		logger.Printf("handleChunkDataRequest: error sending response: %s", err)
	}

}

// validation functions, They respond to the request if there is an error
func validateChunkStringHeader(req *nats.Msg, headerName string, errStatus int) (string, error) {
	header := req.Header.Get(headerName)
	if header == "" {

		natsErr := NatsServiceError{
			Status:        errStatus,
			ErrorMessage:  "missing header: " + headerName,
			ApiStatusCode: errStatus,
			InternalErr:   "400",
		}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			log.Printf("Unable to marshal error: %s", err)
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set(nats_service_common.STATUS, "400")
		err = req.RespondMsg(&respMsg)

		return "", fmt.Errorf("missing header: %s", headerName)
	}
	return header, nil
}
func validateChunkIntHeader(req *nats.Msg, headerName string, errStatus int) (int, error) {
	header := req.Header.Get(headerName)
	if header == "" {

		natsErr := NatsServiceError{
			Status:        errStatus,
			ErrorMessage:  "missing header: " + headerName,
			ApiStatusCode: errStatus,
			InternalErr:   "400",
		}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			log.Printf("Unable to marshal error: %s", err)
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set(nats_service_common.STATUS, "400")
		err = req.RespondMsg(&respMsg)

		return -1, fmt.Errorf("missing header: %s", headerName)
	}
	intHeader, err := strconv.Atoi(header)
	if err != nil {
		natsErr := NatsServiceError{
			Status:        errStatus,
			ErrorMessage:  "missing header: " + headerName,
			ApiStatusCode: errStatus,
			InternalErr:   "400",
		}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			log.Printf("Unable to marshal error: %s", err)
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set(nats_service_common.STATUS, "400")
		err = req.RespondMsg(&respMsg)

		return -1, fmt.Errorf("header: %s must be an integer", headerName)
	}
	return intHeader, nil
}
func (ns *NatService) validateAndGetChunkRequestInfo(chunkReq *nats.Msg) (string, int, *log.Logger, error) {
	//check if header is not null
	if chunkReq.Header == nil {
		natsErr := NatsServiceError{Status: 400, ErrorMessage: "missing header", ApiStatusCode: 400}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			return "", -1, nil, err
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set(nats_service_common.STATUS, "400")
		err = chunkReq.RespondMsg(&respMsg)
		if err != nil {
			return "", -1, nil, err
		}
		return "", -1, nil, errors.New("missing header")
	}
	chunksId, err := validateChunkStringHeader(chunkReq, nats_service_common.CHUNKS_ID, 400)
	if err != nil {
		return "", -1, nil, err
	}
	chunkIndex, err := validateChunkIntHeader(chunkReq, nats_service_common.CHUNK_INDEX, 400)
	if err != nil {
		return "", -1, nil, err
	}

	chunks := ns.chunkCache.Get(chunksId)
	if chunks == nil {
		natsErr := NatsServiceError{Status: 400, ErrorMessage: fmt.Sprintf("chunksId %s is not in memory", chunkReq.Header.Get(nats_service_common.CHUNKS_ID)), ApiStatusCode: 400}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			return "", -1, nil, err
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set(nats_service_common.STATUS, "400")
		err = chunkReq.RespondMsg(&respMsg)
		if err != nil {
			return "", -1, nil, err
		}
		return "", -1, nil, errors.New("CHUNKS_ID not in cache")
	}

	if chunkIndex > len(chunks.Value()) {
		natsErr := NatsServiceError{Status: 400, ErrorMessage: fmt.Sprintf("chunkIndex %d is out of range", chunkIndex), ApiStatusCode: 400}
		errorData, err := json.Marshal(natsErr)
		if err != nil {
			return "", -1, nil, err
		}
		respMsg := nats.Msg{Data: errorData, Header: nats.Header{}}
		respMsg.Header.Set(nats_service_common.STATUS, "400")
		err = chunkReq.RespondMsg(&respMsg)
		if err != nil {
			return "", -1, nil, err
		}
		return "", -1, nil, errors.New("CHUNK_INDEX out of range")
	}
	//chunksId, chunkIndex, logger, err
	msgId := chunkReq.Header.Get(nats_service_common.MESSAGE_ID)
	if msgId == "" {
		msgId = uuid.New().String()
	}

	return chunkReq.Header.Get(nats_service_common.CHUNKS_ID), chunkIndex, createLogger(msgId), nil

}

// downloadRequestChunks downloads chunked request data from the client.
// This mirrors the client-side downloadChunks but runs on the server to
// reassemble chunked request payloads.
func (ns *NatService) downloadRequestChunks(subject, messageId, chunksId, count string, logger *log.Logger) ([]byte, error) {
	chunksCount, err := strconv.Atoi(count)
	if err != nil {
		logger.Printf("Error parsing request chunks count: %s", err)
		return nil, err
	}

	buff := bytes.NewBuffer(nil)

	for i := 0; i < chunksCount; i++ {
		request := nats.Msg{
			Subject: subject,
			Header:  nats.Header{},
		}

		request.Header.Set(nats_service_common.CHUNKS_ID, chunksId)
		request.Header.Set(nats_service_common.CHUNK_INDEX, strconv.Itoa(i))
		request.Header.Set(nats_service_common.MESSAGE_ID, messageId)

		msg, err := ns.nc.RequestMsg(&request, 30*time.Second)
		if err != nil {
			logger.Printf("Error downloading request chunk %d: %s", i, err)
			return nil, err
		}

		if msg.Header.Get(nats_service_common.STATUS) == "200" {
			buff.Write(msg.Data)
		} else {
			natsError := &NatsServiceError{}
			parsError := json.Unmarshal(msg.Data, natsError)
			if parsError != nil {
				return nil, fmt.Errorf("invalid response from client chunk server: %s, %w", msg.Data, parsError)
			}
			return nil, fmt.Errorf("unable to retrieve request chunk %d: %v", i, natsError)
		}
	}

	return buff.Bytes(), nil
}
