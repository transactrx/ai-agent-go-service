package nats_service

import (
	"encoding/json"
	"log"
	"strings"

	"github.com/nats-io/nats.go"
)

// DiscoverySubject is the subject used for service discovery
const DiscoverySubject = "_discovery.all"

// ApiDocsSubjectSuffix is the suffix for the API documentation subject
const ApiDocsSubjectSuffix = "_api_docs"

// StatsSubjectSuffix is the suffix for the stats endpoint (no queue group - all instances respond)
const StatsSubjectSuffix = "_stats"

// ServiceInfo represents the basic service information returned by discovery
type ServiceInfo struct {
	ServiceName    string `json:"serviceName"`
	SubjectPrefix  string `json:"subjectPrefix"`
	Description    string `json:"description,omitempty"`
	RepositoryURL  string `json:"repositoryUrl,omitempty"`
	ApiDocsSubject string `json:"apiDocsSubject,omitempty"`
}

// ApiDocsResponse represents the full API documentation for a service
type ApiDocsResponse struct {
	ServiceName   string          `json:"serviceName"`
	SubjectPrefix string          `json:"subjectPrefix"`
	Description   string          `json:"description,omitempty"`
	RepositoryURL string          `json:"repositoryUrl,omitempty"`
	StatusCodes   []StatusCodeDoc `json:"statusCodes,omitempty"` // Standard status codes for all endpoints
	Endpoints     []EndpointDoc   `json:"endpoints"`
}

// DiscoveryResponse represents a service's response to a discovery request
// Deprecated: Use ServiceInfo for basic discovery and ApiDocsResponse for full documentation
type DiscoveryResponse struct {
	ServiceName string        `json:"serviceName"`
	BasePath    string        `json:"basePath"`
	Endpoints   []EndpointDoc `json:"endpoints"`
}

// InstanceStatsResponse represents stats from a single service instance
// This is returned by the _stats endpoint (broadcast to all instances)
type InstanceStatsResponse struct {
	ServiceName   string                  `json:"serviceName"`
	InstanceId    string                  `json:"instanceId"`
	SubjectPrefix string                  `json:"subjectPrefix"`
	Uptime        string                  `json:"uptime"`
	Endpoints     []EndpointStatsSnapshot `json:"endpoints"`
}

// HeaderDoc represents documentation for a single header
type HeaderDoc struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
	Example     string `json:"example,omitempty"`
}

// ParameterDoc represents documentation for a single path parameter
type ParameterDoc struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
	Example     string `json:"example,omitempty"`
}

// ResponseDoc represents documentation for an endpoint's response
type ResponseDoc struct {
	Description string      `json:"description,omitempty"`
	ContentType string      `json:"contentType,omitempty"`
	Example     string      `json:"example,omitempty"`
	Headers     []HeaderDoc `json:"headers,omitempty"` // Custom response headers
}

// StatusCodeDoc represents documentation for an HTTP-like status code
type StatusCodeDoc struct {
	Code        int    `json:"code"`
	Description string `json:"description"`
}

// EndpointDoc represents documentation for a single endpoint
type EndpointDoc struct {
	Path           string         `json:"path"`
	FullSubject    string         `json:"fullSubject"`
	ExampleSubject string         `json:"exampleSubject,omitempty"`
	Parameters     []ParameterDoc `json:"parameters,omitempty"`
	Headers        []HeaderDoc    `json:"headers,omitempty"`
	Response       *ResponseDoc   `json:"response,omitempty"`
	Description    string         `json:"description,omitempty"`
	WildcardType   string         `json:"wildcardType,omitempty"` // "single" (*) or "multi" (>)
}

// standardStatusCodes returns the default status codes for all endpoints
func standardStatusCodes() []StatusCodeDoc {
	return []StatusCodeDoc{
		{Code: 200, Description: "Success"},
		{Code: 400, Description: "Validation error"},
		{Code: 403, Description: "Authorization error"},
		{Code: 404, Description: "Endpoint not found"},
		{Code: 500, Description: "Server error"},
	}
}

// registerDiscoveryEndpoint subscribes to the discovery subject.
// If subscription fails (e.g., due to permissions), the service continues
// to operate normally with a warning logged.
func (ns *NatService) registerDiscoveryEndpoint() {
	sub, err := ns.nc.Subscribe(DiscoverySubject, ns.handleDiscoveryRequest)
	if err != nil {
		log.Printf("WARNING: Unable to subscribe to discovery subject '%s': %v. Service will operate normally.", DiscoverySubject, err)
		return
	}

	if !sub.IsValid() {
		log.Printf("WARNING: Discovery subscription to '%s' is invalid. Service will operate normally.", DiscoverySubject)
		return
	}

	log.Printf("Registered discovery endpoint on subject: %s", DiscoverySubject)
	ns.discoverySubscription = sub
}

// handleDiscoveryRequest responds to discovery requests with basic service info
func (ns *NatService) handleDiscoveryRequest(msg *nats.Msg) {
	response := ServiceInfo{
		ServiceName:    ns.basePath,
		SubjectPrefix:  ns.basePath,
		Description:    ns.description,
		RepositoryURL:  ns.repositoryURL,
		ApiDocsSubject: ns.basePath + "." + ApiDocsSubjectSuffix,
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshaling discovery response: %v", err)
		return
	}

	if err := msg.Respond(jsonData); err != nil {
		log.Printf("Error responding to discovery request: %v", err)
	}
}

// registerApiDocsEndpoint registers the API documentation endpoint as an internal endpoint.
// This uses the same routing mechanism as user-defined endpoints.
func (ns *NatService) registerApiDocsEndpoint() {
	err := ns.addEndpointInternal(ApiDocsSubjectSuffix, ns.handleApiDocsRequest, true)
	if err != nil {
		log.Printf("WARNING: Unable to register API docs endpoint: %v. Service will operate normally.", err)
		return
	}
	log.Printf("Registered API docs endpoint on subject: %s.%s", ns.basePath, ApiDocsSubjectSuffix)
}

// handleApiDocsRequest responds to API documentation requests with full endpoint documentation
func (ns *NatService) handleApiDocsRequest(msg *NatsMessage) *NatsServiceError {
	response := ApiDocsResponse{
		ServiceName:   ns.basePath,
		SubjectPrefix: ns.basePath,
		Description:   ns.description,
		RepositoryURL: ns.repositoryURL,
		StatusCodes:   standardStatusCodes(),
		Endpoints:     ns.buildEndpointDocs(),
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		msg.Logger.Printf("Error marshaling API docs response: %v", err)
		svcErr := NewServerError("failed to generate API documentation", 5001, err)
		return &svcErr
	}

	msg.ResponseBody = jsonData
	return nil
}


// buildEndpointDocs creates documentation for all registered endpoints
func (ns *NatService) buildEndpointDocs() []EndpointDoc {
	docs := make([]EndpointDoc, 0, len(ns.endPoints))

	for _, ep := range ns.endPoints {
		// Skip internal endpoints (e.g., _api_docs) - they are not part of the public API
		if ep.internal {
			continue
		}

		doc := EndpointDoc{
			Path:        ep.path,
			FullSubject: ns.basePath + "." + ep.path,
		}

		// Get auto-discovered parameter names from regex
		var autoParams []string
		if ep.paramRegex != nil {
			groupNames := ep.paramRegex.GetGroupNames()
			if len(groupNames) > 1 {
				// Skip group 0 (full match)
				autoParams = groupNames[1:]
			}
		}

		// Merge auto-discovered params with user-provided documentation
		doc.Parameters = mergeParameterDocs(autoParams, ep.parameters)

		// Generate example subject by replacing :param with example values
		doc.ExampleSubject = buildExampleSubject(doc.FullSubject, doc.Parameters)

		// Detect wildcard type
		if ep.pathHasWildcards {
			if strings.HasSuffix(ep.path, ">") {
				doc.WildcardType = "multi"
			} else if strings.Contains(ep.path, "*") {
				doc.WildcardType = "single"
			}
		}

		// Include description if provided
		if ep.description != "" {
			doc.Description = ep.description
		}

		// Include headers if provided
		if len(ep.headers) > 0 {
			doc.Headers = ep.headers
		}

		// Include response documentation if provided
		if ep.response != nil {
			doc.Response = ep.response
		}

		docs = append(docs, doc)
	}

	return docs
}

// buildExampleSubject replaces :param placeholders with example values
func buildExampleSubject(fullSubject string, params []ParameterDoc) string {
	if len(params) == 0 {
		return ""
	}

	example := fullSubject
	hasExample := false

	for _, p := range params {
		placeholder := ":" + p.Name
		if strings.Contains(example, placeholder) {
			replacement := p.Example
			if replacement == "" {
				replacement = "{" + p.Name + "}"
			} else {
				hasExample = true
			}
			example = strings.Replace(example, placeholder, replacement, 1)
		}
	}

	// Only return example if at least one parameter had an example value
	// or if we have parameters (show placeholder format)
	if hasExample || len(params) > 0 {
		return example
	}
	return ""
}

// mergeParameterDocs combines auto-discovered parameter names with user-provided documentation.
// Auto-discovered names ensure all path parameters are included, while user docs provide
// descriptions, required flags, and examples.
func mergeParameterDocs(autoParams []string, userDocs []ParameterDoc) []ParameterDoc {
	if len(autoParams) == 0 {
		return nil
	}

	// Create map from user docs for quick lookup
	userDocMap := make(map[string]ParameterDoc)
	for _, pd := range userDocs {
		userDocMap[pd.Name] = pd
	}

	result := make([]ParameterDoc, 0, len(autoParams))
	for _, name := range autoParams {
		if ud, ok := userDocMap[name]; ok {
			// Use user-provided doc (ensures Name is set correctly)
			ud.Name = name
			result = append(result, ud)
		} else {
			// Create minimal doc from auto-discovered name
			result = append(result, ParameterDoc{Name: name})
		}
	}
	return result
}

// drainDiscoverySubscription drains the discovery subscription if it exists
func (ns *NatService) drainDiscoverySubscription() error {
	if ns.discoverySubscription != nil {
		if err := ns.discoverySubscription.Drain(); err != nil {
			log.Printf("Error draining discovery subscription: %v", err)
			return err
		}
	}
	return nil
}

// registerStatsEndpoint subscribes to the stats subject WITHOUT a queue group.
// This means ALL instances will respond to stats requests, enabling aggregation.
func (ns *NatService) registerStatsEndpoint() {
	statsSubject := ns.basePath + "." + StatsSubjectSuffix

	// Subscribe WITHOUT queue group so ALL instances respond
	sub, err := ns.nc.Subscribe(statsSubject, ns.handleStatsRequest)
	if err != nil {
		log.Printf("WARNING: Unable to subscribe to stats subject '%s': %v. Stats will not be available.", statsSubject, err)
		return
	}

	if !sub.IsValid() {
		log.Printf("WARNING: Stats subscription to '%s' is invalid. Stats will not be available.", statsSubject)
		return
	}

	log.Printf("Registered stats endpoint on subject: %s (broadcast, no queue)", statsSubject)
	ns.statsSubscription = sub
}

// handleStatsRequest responds to stats requests with this instance's statistics
func (ns *NatService) handleStatsRequest(msg *nats.Msg) {
	// Build stats for all endpoints
	endpointStats := make([]EndpointStatsSnapshot, 0, len(ns.endpointStats))
	for _, stats := range ns.endpointStats {
		endpointStats = append(endpointStats, stats.GetStats())
	}

	response := InstanceStatsResponse{
		ServiceName:   ns.basePath,
		InstanceId:    ns.instanceId,
		SubjectPrefix: ns.basePath,
		Uptime:        ns.getUptime(),
		Endpoints:     endpointStats,
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		log.Printf("Error marshaling stats response: %v", err)
		return
	}

	if err := msg.Respond(jsonData); err != nil {
		log.Printf("Error responding to stats request: %v", err)
	}
}

// getOrCreateEndpointStats returns the stats tracker for an endpoint, creating it if needed
func (ns *NatService) getOrCreateEndpointStats(path string) *EndPointStats {
	if stats, ok := ns.endpointStats[path]; ok {
		return stats
	}
	stats := NewEndPointStats(ns.basePath + "." + path)
	ns.endpointStats[path] = stats
	return stats
}

// drainStatsSubscription drains the stats subscription if it exists
func (ns *NatService) drainStatsSubscription() error {
	if ns.statsSubscription != nil {
		if err := ns.statsSubscription.Drain(); err != nil {
			log.Printf("Error draining stats subscription: %v", err)
			return err
		}
	}
	return nil
}
