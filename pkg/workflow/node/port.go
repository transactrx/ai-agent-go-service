package node

// PortDir indicates which side of a node a port belongs to.
type PortDir int

const (
	PortIn  PortDir = 1
	PortOut PortDir = 2
)

// Cardinality bounds the number of connections allowed on a port.
type Cardinality int

const (
	CardOne        Cardinality = 1
	CardZeroOrOne  Cardinality = 2
	CardZeroOrMany Cardinality = 3
)

// PortSpec describes a single typed port a node exposes.
type PortSpec struct {
	Name        string
	Direction   PortDir
	Cardinality Cardinality
	Required    bool
	PeerRole    Role // role the peer must implement (empty for "main")
	Description string
}

// Standard port-name constants. Workflow JSON connections must use these
// strings for the typed reference ports.
const (
	PortMain            = "main"
	PortAILanguageModel = "ai_languageModel"
	PortAIMemory        = "ai_memory"
	PortAITool          = "ai_tool"
	PortPolicy          = "policy"
)
