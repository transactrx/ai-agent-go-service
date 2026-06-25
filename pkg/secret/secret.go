// Package secret prevents accidental disclosure of sensitive values. The String
// type redacts itself on every default formatting and serialization path; the
// only way to retrieve the raw value is the explicit Reveal() method, which is
// greppable at code-review time.
package secret

import "fmt"

// String holds a sensitive value. All standard stringification paths return
// "<redacted>" so it is safe to pass to log.Printf, fmt.Println, JSON marshaling
// fallthroughs, etc. Use Reveal() at the boundary that genuinely needs the raw
// value (HTTP basic auth, DB connection string, etc.).
type String struct {
	val string
	set bool
}

// New wraps a raw value as a redacting String.
func New(v string) String { return String{val: v, set: true} }

// IsSet reports whether the value was explicitly set via New.
func (s String) IsSet() bool { return s.set }

// Reveal returns the raw string. Greppable in code review.
func (s String) Reveal() string { return s.val }

// String satisfies fmt.Stringer.
func (s String) String() string { return "<redacted>" }

// Format satisfies fmt.Formatter so %v, %s, %q all redact.
func (s String) Format(f fmt.State, _ rune) { _, _ = fmt.Fprint(f, "<redacted>") }

// MarshalJSON keeps the value out of any default JSON encoding.
func (s String) MarshalJSON() ([]byte, error) { return []byte(`"<redacted>"`), nil }

// MarshalText keeps the value out of text/encoders (e.g., env-style serializers).
func (s String) MarshalText() ([]byte, error) { return []byte("<redacted>"), nil }
