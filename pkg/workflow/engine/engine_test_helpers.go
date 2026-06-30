package engine

// NewForTest builds an Engine whose workflow map is the given value. It exists
// only so packages outside engine (e.g. promptadmin) can exercise read-only
// helpers like Workflow/ApplyPromptUpdate against hand-built workflows without
// a full LoadAll. The workflows field is unexported, so this is the least
// invasive way to seed it from another package's tests.
func NewForTest(workflows map[string]*Workflow) *Engine {
	return &Engine{workflows: workflows}
}
