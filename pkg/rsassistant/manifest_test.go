package rsassistant

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestBuildCardSpecDefaultsFromWorkflow(t *testing.T) {
	spec, err := BuildCardSpec("SinglePowerlineSearch", "Pharmacy claim search agent.", nil, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Name != "SinglePowerlineSearch" || spec.DisplayName != "SinglePowerlineSearch" ||
		spec.Description != "Pharmacy claim search agent." || spec.Version != "1.0.0" {
		t.Fatalf("unexpected defaults %+v", spec)
	}
	if len(spec.Tags) != 0 || len(spec.Skills) != 0 {
		t.Fatalf("tags/skills must default empty: %+v", spec)
	}
}

func TestBuildCardSpecEmptyDescriptionFallsBack(t *testing.T) {
	spec, err := BuildCardSpec("wf", "", nil, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Description != "Workflow wf" {
		t.Fatalf("Description = %q", spec.Description)
	}
}

func TestBuildCardSpecManifestOverrides(t *testing.T) {
	raw := json.RawMessage(`{"name":"powerlineClaimSearch","displayName":"PowerLine Claim Search",
	  "description":"Claims Q&A","version":"3.0.0","tags":["powerline"],
	  "skills":[{"name":"claim-search","description":"search claims","examples":["how many today?"]}]}`)
	spec, err := BuildCardSpec("SinglePowerlineSearch", "ignored", raw, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	if spec.Name != "powerlineClaimSearch" || spec.DisplayName != "PowerLine Claim Search" ||
		spec.Description != "Claims Q&A" || spec.Version != "3.0.0" {
		t.Fatalf("overrides not applied: %+v", spec)
	}
	if len(spec.Tags) != 1 || len(spec.Skills) != 1 || spec.Skills[0].Examples[0] != "how many today?" {
		t.Fatalf("tags/skills not applied: %+v", spec)
	}
}

func TestBuildCardSpecDisabled(t *testing.T) {
	_, err := BuildCardSpec("wf", "d", json.RawMessage(`{"enabled":false}`), "1.0.0")
	if !errors.Is(err, ErrDisabled) {
		t.Fatalf("want ErrDisabled, got %v", err)
	}
}

func TestBuildCardSpecRejectsBadNames(t *testing.T) {
	for _, bad := range []string{"has space", "dots.not.ok", "discover", "announce", "slash/x"} {
		raw := json.RawMessage(`{"name":"` + bad + `"}`)
		if _, err := BuildCardSpec("wf", "d", raw, "1.0.0"); err == nil {
			t.Fatalf("name %q must be rejected", bad)
		}
	}
	if _, err := BuildCardSpec("bad id with spaces", "d", nil, "1.0.0"); err == nil {
		t.Fatal("workflow id used as name must also be validated")
	}
}

func TestParseManifestRejectsMalformed(t *testing.T) {
	if _, err := ParseManifest(json.RawMessage(`{"name": 5}`)); err == nil {
		t.Fatal("non-string name must error")
	}
	m, err := ParseManifest(nil)
	if err != nil || m.Name != "" || m.Enabled != nil {
		t.Fatalf("nil raw must yield zero manifest, got %+v err=%v", m, err)
	}
}
