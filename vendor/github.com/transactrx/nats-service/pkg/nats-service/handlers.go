package nats_service

import (
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

// GetTime is a handler function that returns the current time.
func GetTime(msg *NatsMessage) *NatsServiceError {
	if msg.Logger == nil {
		msg.Logger = log.New(io.Discard, "", 0)
	}
	// msg.Data is the request payload. Handlers should populate msg.ResponseBody.
	// msg.Logger.Printf("handler GetTime received message with data: %s", string(msg.Data))
	msg.Logger.Printf("handler GetTime received message")
	msg.ResponseBody = []byte(fmt.Sprintf("current time: %s", time.Now().Format(time.RFC3339)))
	return nil
}

// GetTimeError is a handler function that simulates an error.
func GetTimeError(msg *NatsMessage) *NatsServiceError {
	if msg.Logger == nil {
		msg.Logger = log.New(io.Discard, "", 0)
	}
	// msg.Logger.Printf("handler GetTimeError received message with data: %s", string(msg.Data))
	msg.Logger.Printf("handler GetTimeError received message")
	// Using NewServerError as an example. The ApiStatusCode can be used for more specific internal codes.
	// The original ErrSubCodeTimeError doesn't directly map to the current NatsServiceError structure.
	// We'll use ApiStatusCode to represent this specific error type if needed,
	// and ErrorMessage for the public-facing error.
	return &NatsServiceError{
		Status:        500, // Internal Server Error type
		ErrorMessage:  "application error: simulated error getting time",
		ApiStatusCode: 1001, // Example: Custom API status code for "TimeError"
		InternalErr:   "simulated error getting time",
	}
}

// GetCompressedResponse is a handler function that returns a compressed response.
// If msg.Data is not empty, it will compress msg.Data.
// Otherwise, it will attempt to read and compress the file specified by the "filePath" parameter in msg.Parameters.
func GetCompressedResponse(msg *NatsMessage) *NatsServiceError {
	if msg.Logger == nil {
		msg.Logger = log.New(io.Discard, "", 0)
	}
	msg.Logger.Printf("handler GetCompressedResponse received message with params: %v", msg.Parameters)

	var dataToCompress []byte
	var err error

	// Use msg.Body for request payload if it's not nil.
	// If msg.Body is nil, then try filePath.
	// If msg.Body is an empty slice ([]byte{}), it should be compressed.
	if msg.Body != nil {
		dataToCompress = msg.Body
		msg.Logger.Printf("Compressing data from NatsMessage.Body (length: %d)", len(dataToCompress))
	} else {
		filePath := msg.Parameters["filePath"]
		if filePath == "" {
			// Using NewValidationError as an example
			return &NatsServiceError{
				Status:        400, // Bad Request
				ErrorMessage:  "bad request: filePath parameter is missing and NatsMessage.Body is empty",
				ApiStatusCode: 1002, // Example: Custom API status code for MissingParam
				InternalErr:   "filePath parameter is missing and NatsMessage.Body is empty",
			}
		}
		msg.Logger.Printf("Reading file for compression: %s", filePath)
		dataToCompress, err = os.ReadFile(filePath)
		if err != nil {
			msg.Logger.Printf("Error reading file %s: %v", filePath, err)
			return &NatsServiceError{
				Status:        500, // Internal Server Error
				ErrorMessage:  "file read error",
				ApiStatusCode: 1003, // Example: Custom API status code for FileReadError
				InternalErr:   fmt.Sprintf("error reading file %s: %v", filePath, err),
			}
		}
	}

	// Create a pipe
	pr, pw := io.Pipe()

	// Create a gzip writer
	gw := gzip.NewWriter(pw)

	// Create a channel to signal completion and to pass potential errors from the goroutine.
	errChan := make(chan error, 1)

	go func() {
		defer pw.Close() // Ensure pipe writer is closed to signal EOF to reader.
		defer gw.Close() // Ensure gzip writer is closed to flush all data.

		_, errWrite := gw.Write(dataToCompress)
		if errWrite != nil {
			msg.Logger.Printf("Error compressing data: %v", errWrite)
			errChan <- errWrite // Send error to main goroutine.
			return
		}
		errChan <- nil // Signal success.
	}()

	compressedData, errRead := io.ReadAll(pr)
	// Wait for the goroutine to finish and check for errors.
	compressionGoroutineError := <-errChan

	if compressionGoroutineError != nil {
		msg.Logger.Printf("Error during compression goroutine: %v", compressionGoroutineError)
		return &NatsServiceError{
			Status:        500,
			ErrorMessage:  "compression error",
			ApiStatusCode: 1004, // Example: Custom API status code for CompressionError
			InternalErr:   fmt.Sprintf("error during data compression: %v", compressionGoroutineError),
		}
	}

	if errRead != nil {
		msg.Logger.Printf("Error reading compressed data from pipe: %v", errRead)
		return &NatsServiceError{
			Status:        500,
			ErrorMessage:  "compression error",
			ApiStatusCode: 1004, // Example: Custom API status code for CompressionError
			InternalErr:   fmt.Sprintf("error reading compressed data: %v", errRead),
		}
	}

	if len(compressedData) == 0 && len(dataToCompress) > 0 {
		msg.Logger.Printf("Warning: Compressed data is empty, but original data was not. Original size: %d", len(dataToCompress))
		// This might indicate an issue if not expected, but not necessarily an error for all cases (e.g. if original data was empty and compressible)
	}

	msg.ResponseBody = compressedData
	// msg.Data = nil // Request data should remain untouched. Response is in ResponseBody.
	// The Compressed flag is not part of NatsMessage struct. Compression is indicated by headers in the actual service.
	msg.Logger.Printf("Successfully compressed data. Original size: %d, Compressed size: %d", len(dataToCompress), len(msg.ResponseBody))
	return nil
}
