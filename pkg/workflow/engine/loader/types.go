package loader

import "encoding/json"

// rawWorkflow is the parsed envelope of a workflow JSON file.
type rawWorkflow struct {
	Schema      string           `json:"$schema,omitempty"`
	ID          string           `json:"id"`
	Version     int              `json:"version"`
	Description string           `json:"description,omitempty"`
	Trigger     string           `json:"trigger"`
	Nodes       []rawNode        `json:"nodes"`
	Connections []rawConnection  `json:"connections"`
}

// rawNode is one entry in workflow.nodes.
type rawNode struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Name        string          `json:"name,omitempty"`
	Description string          `json:"description,omitempty"`
	Config      json.RawMessage `json:"config"`
	Retry       json.RawMessage `json:"retry,omitempty"`
}

// rawConnection is one entry in workflow.connections.
type rawConnection struct {
	From rawEndpoint `json:"from"`
	To   rawEndpoint `json:"to"`
}

type rawEndpoint struct {
	Node string `json:"node"`
	Port string `json:"port"`
}
