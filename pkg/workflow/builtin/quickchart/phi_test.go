package quickchart

import (
	"encoding/json"
	"testing"
)

func TestScrubFindsLabelFieldName(t *testing.T) {
	data := json.RawMessage(`{"labels":["firstName","claimCount"],"datasets":[{"label":"All","data":[1,2]}]}`)
	hit, err := ScanForPHI(data, []string{"firstName", "lastName", "memberId"})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if hit == "" {
		t.Fatal("expected firstName hit")
	}
	if hit != "firstName" {
		t.Fatalf("expected hit=firstName, got %q", hit)
	}
}

func TestScrubFindsDatasetLabel(t *testing.T) {
	data := json.RawMessage(`{"labels":["a","b"],"datasets":[{"label":"memberId","data":[1,2]}]}`)
	hit, _ := ScanForPHI(data, []string{"memberId"})
	if hit != "memberId" {
		t.Fatalf("expected hit=memberId, got %q", hit)
	}
}

func TestScrubFindsDatasetObjectKey(t *testing.T) {
	data := json.RawMessage(`{"labels":["a"],"datasets":[{"label":"x","data":[1],"phone":"555"}]}`)
	hit, _ := ScanForPHI(data, []string{"phone"})
	if hit != "phone" {
		t.Fatalf("expected hit=phone, got %q", hit)
	}
}

func TestScrubIsWholeTokenNotSubstring(t *testing.T) {
	data := json.RawMessage(`{"labels":["memberCount"],"datasets":[{"label":"x","data":[1]}]}`)
	hit, _ := ScanForPHI(data, []string{"member"})
	if hit != "" {
		t.Fatalf("substring should NOT match, got hit=%q", hit)
	}
}

func TestScrubIsCaseInsensitive(t *testing.T) {
	data := json.RawMessage(`{"labels":["FirstName"],"datasets":[{"label":"x","data":[1]}]}`)
	hit, _ := ScanForPHI(data, []string{"firstName"})
	if hit != "firstName" {
		t.Fatalf("expected case-insensitive match, got hit=%q", hit)
	}
}

func TestScrubAllDefaultFields(t *testing.T) {
	defaults := []string{"firstName", "lastName", "memberId", "dob", "dateOfBirth", "ssn", "address", "phone", "email"}
	for _, f := range defaults {
		data := json.RawMessage(`{"labels":["` + f + `"],"datasets":[{"label":"x","data":[1]}]}`)
		hit, _ := ScanForPHI(data, defaults)
		if hit != f {
			t.Errorf("expected hit=%q, got %q", f, hit)
		}
	}
}

func TestScrubCleanData(t *testing.T) {
	data := json.RawMessage(`{"labels":["mon","tue","wed"],"datasets":[{"label":"errors","data":[1,2,3]}]}`)
	hit, _ := ScanForPHI(data, []string{"firstName", "lastName", "memberId"})
	if hit != "" {
		t.Fatalf("clean data should not match, got hit=%q", hit)
	}
}
