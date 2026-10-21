// Package toolresult encodes builtin tool results for the model.
package toolresult

import (
	"bytes"
	"encoding/json"
)

// Marshal is json.Marshal without HTML escaping. json.Marshal rewrites
// & < > as & < >, which models copy verbatim into answers and
// break URLs (e.g. presigned S3 links). Valid JSON either way.
func Marshal(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
