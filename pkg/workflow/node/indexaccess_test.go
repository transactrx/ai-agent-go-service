package node

import "testing"

func TestIndexAccessKindZeroValueIsNone(t *testing.T) {
	cases := []struct {
		name string
		in   IndexAccess
		want IndexAccessStatus
	}{
		{"zero value", IndexAccess{}, IndexAccessNone},
		{"none", IndexAccess{Status: IndexAccessNone}, IndexAccessNone},
		{"bound", IndexAccess{Status: IndexAccessBound, FunctionID: "F"}, IndexAccessBound},
		{"unbound", IndexAccess{Status: IndexAccessUnbound, Cause: "c"}, IndexAccessUnbound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.in.Kind(); got != tc.want {
				t.Fatalf("Kind() = %q, want %q", got, tc.want)
			}
		})
	}
}
