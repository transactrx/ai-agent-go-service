package rsassistant

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrDisabled is returned by BuildCardSpec when the manifest sets enabled=false.
var ErrDisabled = errors.New("rsassistant: disabled by manifest")

// nameRe mirrors nats-agent's agent-name rule (pkg/agent/agent.go nameRe).
var nameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// reservedNames mirrors nats-agent's reserved agent names.
var reservedNames = map[string]bool{"discover": true, "announce": true}

// Skill is one advertised capability on the agent card.
type Skill struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Examples    []string `json:"examples,omitempty"`
}

// Manifest is the optional top-level "rsassistant" block of a workflow file.
// Every field is optional; see BuildCardSpec for defaults.
type Manifest struct {
	Enabled     *bool    `json:"enabled,omitempty"`
	Name        string   `json:"name,omitempty"`
	DisplayName string   `json:"displayName,omitempty"`
	Description string   `json:"description,omitempty"`
	Version     string   `json:"version,omitempty"`
	Tags        []string `json:"tags,omitempty"`
	Skills      []Skill  `json:"skills,omitempty"`
}

// CardSpec is the resolved, validated description of one published agent.
type CardSpec struct {
	Name        string
	DisplayName string
	Description string
	Version     string
	Tags        []string
	Skills      []Skill
}

// ParseManifest decodes the raw block. nil or empty yields the zero Manifest.
func ParseManifest(raw json.RawMessage) (Manifest, error) {
	var m Manifest
	if len(raw) == 0 {
		return m, nil
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return m, fmt.Errorf("rsassistant manifest: %w", err)
	}
	return m, nil
}

// BuildCardSpec resolves manifest values over workflow defaults and validates
// the agent name. Returns ErrDisabled when the manifest opts out.
func BuildCardSpec(workflowID, workflowDescription string, raw json.RawMessage, defaultVersion string) (CardSpec, error) {
	m, err := ParseManifest(raw)
	if err != nil {
		return CardSpec{}, err
	}
	if m.Enabled != nil && !*m.Enabled {
		return CardSpec{}, ErrDisabled
	}
	spec := CardSpec{
		Name:        strings.TrimSpace(m.Name),
		DisplayName: strings.TrimSpace(m.DisplayName),
		Description: strings.TrimSpace(m.Description),
		Version:     strings.TrimSpace(m.Version),
		Tags:        m.Tags,
		Skills:      m.Skills,
	}
	if spec.Name == "" {
		spec.Name = workflowID
	}
	if !nameRe.MatchString(spec.Name) {
		return CardSpec{}, fmt.Errorf("rsassistant: agent name %q must match %s", spec.Name, nameRe)
	}
	if reservedNames[spec.Name] {
		return CardSpec{}, fmt.Errorf("rsassistant: agent name %q is reserved", spec.Name)
	}
	if spec.DisplayName == "" {
		spec.DisplayName = spec.Name
	}
	if spec.Description == "" {
		spec.Description = strings.TrimSpace(workflowDescription)
	}
	if spec.Description == "" {
		spec.Description = "Workflow " + workflowID
	}
	if spec.Version == "" {
		spec.Version = defaultVersion
	}
	if spec.Tags == nil {
		spec.Tags = []string{}
	}
	if spec.Skills == nil {
		spec.Skills = []Skill{}
	}
	return spec, nil
}
