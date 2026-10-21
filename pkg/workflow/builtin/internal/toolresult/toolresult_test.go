package toolresult

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestMarshal_NoHTMLEscape(t *testing.T) {
	in := map[string]string{"imageUrl": "https://s3.example/c.png?X-Amz-Algorithm=AWS4&X-Amz-Credential=a<b>c"}
	out, err := Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{`\u0026`, `\u003c`, `\u003e`} {
		if bytes.Contains(out, []byte(bad)) {
			t.Errorf("output contains %s: %s", bad, out)
		}
	}
	if !bytes.Contains(out, []byte("AWS4&X-Amz-Credential=a<b>c")) {
		t.Errorf("raw chars missing: %s", out)
	}
	if bytes.HasSuffix(out, []byte("\n")) {
		t.Errorf("trailing newline not trimmed")
	}
	var back map[string]string
	if err := json.Unmarshal(out, &back); err != nil || !reflect.DeepEqual(back, in) {
		t.Errorf("round trip: %v %v", back, err)
	}
}
