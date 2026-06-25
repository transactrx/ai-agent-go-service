package nats_service_common

import (
	"bytes"
	"compress/gzip"
	"log"
)

const COMPRESSED_HEADER = "_Compression"
const GZIP_COMPRESSION_TYPE = "gzip"

const STATUS = "status"

const CHUNKS_ID = "_Chunks_Id"
const CHUNKED_LENGTH = "_Chunked_Length"
const CHUNKED_SUBJECT = "_Chunked_Subject"
const CHUNK_INDEX = "_Chunk_Index"
const MESSAGE_ID = "_Message_Id"
const USER_ID = "_User_Id"

// GZipBytes function gzip byte array
func GZipBytes(toBegzipped []byte) []byte {
	var b bytes.Buffer

	w := gzip.NewWriter(&b)

	_, err := w.Write(toBegzipped)
	if err != nil {
		log.Println(err)
	}
	w.Close()

	return b.Bytes()
}

func GUnzipBytes(toUnzip []byte) ([]byte, error) {
	b := bytes.NewBuffer(toUnzip)
	var r *gzip.Reader

	r, _ = gzip.NewReader(b)
	defer r.Close()

	var out bytes.Buffer
	_, err := out.ReadFrom(r)
	return out.Bytes(), err
}

func ChunkByteArray(data []byte, chunkSize int) [][]byte {
	var chunkedData [][]byte
	for i := 0; i < len(data); i += chunkSize {
		end := i + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunkedData = append(chunkedData, data[i:end])
	}
	return chunkedData
}
