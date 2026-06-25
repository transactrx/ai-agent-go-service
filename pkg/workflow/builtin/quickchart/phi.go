package quickchart

import (
	"encoding/json"
	"strings"
)

// scanForPHI walks the Chart.js data object looking for schema-level PHI
// field-name occurrences in: labels[*] strings, datasets[*].label strings,
// and any object-key on datasets[*]. Match is case-insensitive whole-token.
// Returns the first field that matched (or "" if none), in the canonical
// casing from the scrubFields list.
func ScanForPHI(data json.RawMessage, scrubFields []string) (string, error) {
	if len(scrubFields) == 0 {
		return "", nil
	}
	var d struct {
		Labels   []json.RawMessage            `json:"labels"`
		Datasets []map[string]json.RawMessage `json:"datasets"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return "", err
	}
	// Build a lowercased lookup table to canonical casing.
	canon := make(map[string]string, len(scrubFields))
	for _, f := range scrubFields {
		canon[strings.ToLower(f)] = f
	}

	matchToken := func(s string) string {
		if c, ok := canon[strings.ToLower(s)]; ok {
			return c
		}
		return ""
	}

	for _, lbl := range d.Labels {
		var s string
		if err := json.Unmarshal(lbl, &s); err == nil {
			if c := matchToken(s); c != "" {
				return c, nil
			}
		}
	}
	for _, ds := range d.Datasets {
		for k, v := range ds {
			if c := matchToken(k); c != "" {
				return c, nil
			}
			if k == "label" {
				var s string
				if err := json.Unmarshal(v, &s); err == nil {
					if c := matchToken(s); c != "" {
						return c, nil
					}
				}
			}
		}
	}
	return "", nil
}
