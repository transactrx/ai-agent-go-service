package nats_service

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/dlclark/regexp2"
	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"
	"github.com/nats-io/nats.go"
	nats_service_common "github.com/transactrx/nats-service/pkg/nats-service-common"
)

type NatService struct {
	url                         string
	nc                          *nats.Conn
	subscription                *nats.Subscription
	chunkedSubscription         *nats.Subscription
	chunkedReceiverSubscription *nats.Subscription
	discoverySubscription       *nats.Subscription
	statsSubscription           *nats.Subscription
	chunkCache                  *ttlcache.Cache[string, [][]byte]
	endPoints                   []*NatsEndpoint
	endpointStats               map[string]*EndPointStats // stats per endpoint path
	basePath                    string
	queueName                   string
	description                 string
	repositoryURL               string
	instanceId                  string
	startTime                   time.Time
	maxRespSizeToCompress       int
	maxRespSizeToChunk          int
	debug                       bool
}

type NatsMessage struct {
	Body            []byte
	Header          nats.Header
	Path            string
	ResponseBody    []byte
	MessageId       string
	UserId          string
	ResponseHeader  nats.Header
	Logger          *log.Logger
	Parameters      map[string]string
	OriginalMessage *nats.Msg
}

type NatsEndpoint struct {
	path             string
	endPointFunc     NatsEndpointFunc
	paramRegex       *regexp2.Regexp
	matchRegex       *regexp2.Regexp
	pathSeparator    string
	pathHasWildcards bool
	description      string
	headers          []HeaderDoc
	parameters       []ParameterDoc
	response         *ResponseDoc
	internal         bool // true for library-reserved endpoints (e.g., _api_docs)
}

type NatsEndpointFunc func(msg *NatsMessage) *NatsServiceError

var ConfigError = errors.New("configuration error")

func New(basePath string) (*NatService, error) {
	natsUrl := getEnvironmentVariableOrPanic("NATS_URL")
	natsQueueName := getEnvironmentVariableOrPanic("NATS_QUEUE_NAME")
	natsDebug := os.Getenv("NATS_DEBUG")
	natsToken := os.Getenv("NATS_JWT")
	natsKey := os.Getenv("NATS_KEY")

	debugEnabled, _ := strconv.ParseBool(natsDebug)

	return NewLowLevelDebug(basePath, natsQueueName, natsUrl, natsToken, natsKey, 1024*2, 1024*300, debugEnabled)
}

func (ns *NatService) GetNatsService() *nats.Conn {
	return ns.nc
}

// SetDescription sets a description for the service that will be included in discovery responses.
// This should be called before Start() to ensure the description is available during discovery.
func (ns *NatService) SetDescription(description string) {
	ns.description = description
}

// SetRepositoryURL sets the git repository URL for the service source code.
// This helps users and coding agents find the source to better understand the microservice.
// This should be called before Start() to ensure the URL is available during discovery.
func (ns *NatService) SetRepositoryURL(url string) {
	ns.repositoryURL = url
}

// GetInstanceId returns the unique identifier for this service instance
func (ns *NatService) GetInstanceId() string {
	return ns.instanceId
}

// getUptime returns the service uptime as a human-readable string
func (ns *NatService) getUptime() string {
	return time.Since(ns.startTime).Round(time.Second).String()
}

func NewLowLevel(basePath, natsQueueName, natsUrl, natsToken, natsKey string, maxRespSizeToCompress, maxRespSizeToChunk int) (*NatService, error) {
	natsDebug := os.Getenv("NATS_DEBUG")

	debug, _ := strconv.ParseBool(natsDebug)
	return NewLowLevelDebug(basePath, natsQueueName, natsUrl, natsToken, natsKey, maxRespSizeToCompress, maxRespSizeToChunk, debug)
}

func NewLowLevelDebug(basePath, natsQueueName, natsUrl, natsToken, natsKey string, maxRespSizeToCompress, maxRespSizeToChunk int, debug bool) (*NatService, error) {

	var opts []nats.Option
	if natsToken != "" && natsKey != "" {
		//Add authorization when configured
		opts = []nats.Option{nats.UserJWTAndSeed(natsToken, natsKey)}
	}
	opts = setupConnOptions(opts)
	nc, err := nats.Connect(natsUrl, opts...)
	if err != nil {
		log.Printf("ERROR: Failed to connect to NATS server at %s: %s", natsUrl, err)
		return nil, err
	}
	log.Printf("Connected to NATS server at %s", natsUrl)

	// Generate instance ID: hostname-shortUUID
	hostname, _ := os.Hostname()
	shortId := uuid.New().String()[:8]
	instanceId := fmt.Sprintf("%s-%s", hostname, shortId)

	ns := NatService{
		url:                   natsUrl,
		nc:                    nc,
		basePath:              basePath,
		queueName:             natsQueueName,
		maxRespSizeToCompress: maxRespSizeToCompress,
		maxRespSizeToChunk:    maxRespSizeToChunk,
		chunkCache:            ttlcache.New[string, [][]byte](),
		endpointStats:         make(map[string]*EndPointStats),
		instanceId:            instanceId,
		startTime:             time.Now(),
		debug:                 debug,
	}

	return &ns, nil
}

func (ns *NatService) AddEndpoint(path string, endPoint NatsEndpointFunc) error {
	return ns.addEndpointInternal(path, endPoint, false)
}

// addEndpointInternal is the internal implementation for adding endpoints.
// The internal flag allows the library to register reserved endpoints.
func (ns *NatService) addEndpointInternal(path string, endPoint NatsEndpointFunc, internal bool) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("endpoint path cannot be empty")
	}

	if endPoint == nil {
		return fmt.Errorf("endpoint cannot be nil: %w", ConfigError)
	}

	// Check for reserved suffixes (only for external registrations)
	if !internal && strings.HasSuffix(path, ApiDocsSubjectSuffix) {
		return fmt.Errorf("endpoint path '%s' uses reserved suffix '%s': %w", path, ApiDocsSubjectSuffix, ConfigError)
	}

	pathSeparator := "."
	if strings.Contains(path, "/") {
		pathSeparator = "/"
	}

	for _, endPoint := range ns.endPoints {
		if endPoint.path == path {
			return fmt.Errorf("endpoint already in use: %w", ConfigError)
		}
	}

	natsEndpoint := NatsEndpoint{
		path:          path,
		endPointFunc:  endPoint,
		pathSeparator: pathSeparator,
		internal:      internal,
	}

	hasWildcardSuffix := strings.HasSuffix(path, ">")
	if hasWildcardSuffix || strings.ContainsAny(path, "*") {
		// Escape periods since they are special in regex
		matchPath := strings.ReplaceAll(path, ".", `\.`)
		// * in NATS means match anything between 2 periods
		matchPath = strings.ReplaceAll(matchPath, "*", "[^.]+")
		if hasWildcardSuffix {
			// > suffix in NATS means replace anything after preceding period
			// We simply remove the suffix - the expression will match anything until end of string
			matchPath = "^" + ns.basePath + "." + strings.TrimSuffix(matchPath, ">")
		} else {
			// Adding $ suffix ensures match on the exact same number of previously escaped subject tokens
			matchPath = "^" + ns.basePath + "." + matchPath + "$"
		}

		var err error
		natsEndpoint.matchRegex, err = regexp2.Compile(matchPath, regexp2.RE2)
		if err != nil {
			return fmt.Errorf("invalid regex expression: %w", err)
		}

		natsEndpoint.pathHasWildcards = true
		natsEndpoint.paramRegex = nil
	} else {
		fullPath := ns.basePath + "." + path
		matchFullPath := ns.basePath + "." + strings.Split(path, pathSeparator)[0]

		var err error
		natsEndpoint.matchRegex, err = regexp2.Compile("^"+matchFullPath+"$", regexp2.RE2)
		if err != nil {
			return fmt.Errorf("invalid regex expression: %w", err)
		}

		natsEndpoint.pathHasWildcards = false

		natsEndpoint.paramRegex, err = convertToRegex(fullPath, pathSeparator)
		if err != nil {
			return fmt.Errorf("invalid regex expression: %w", err)
		}
	}

	ns.endPoints = append(ns.endPoints, &natsEndpoint)

	return nil
}

// AddEndpointWithDoc registers an endpoint with documentation for discovery.
// The description, headers, and parameters are included in discovery responses
// to help clients understand the endpoint's purpose and requirements.
// Pass nil for headers or params if not needed.
func (ns *NatService) AddEndpointWithDoc(path string, description string, headers []HeaderDoc, params []ParameterDoc, response *ResponseDoc, endPoint NatsEndpointFunc) error {
	err := ns.AddEndpoint(path, endPoint)
	if err != nil {
		return err
	}

	// Set documentation on the last added endpoint
	lastEp := ns.endPoints[len(ns.endPoints)-1]
	lastEp.description = description
	lastEp.headers = headers
	lastEp.parameters = params
	lastEp.response = response
	return nil
}

// EndpointRegistration represents a single endpoint registration with its documentation.
type EndpointRegistration struct {
	Path        string
	Description string
	Headers     []HeaderDoc
	Parameters  []ParameterDoc
	Response    *ResponseDoc
	Handler     NatsEndpointFunc
}

// AddEndpointWithDocs registers multiple endpoints at once, each with its own documentation.
// This is useful for batch registration of documented endpoints.
// If any endpoint fails to register, the function returns immediately with the error
// and any previously registered endpoints in this batch remain registered.
func (ns *NatService) AddEndpointWithDocs(endpoints []EndpointRegistration) error {
	for _, ep := range endpoints {
		if err := ns.AddEndpoint(ep.Path, ep.Handler); err != nil {
			return fmt.Errorf("failed to register endpoint '%s': %w", ep.Path, err)
		}
		// Set documentation on the last added endpoint
		lastEp := ns.endPoints[len(ns.endPoints)-1]
		lastEp.description = ep.Description
		lastEp.headers = ep.Headers
		lastEp.parameters = ep.Parameters
		lastEp.response = ep.Response
	}
	return nil
}

func (ns *NatService) Start() error {

	if len(ns.endPoints) == 0 {
		return fmt.Errorf("no endpoints configured")
	}

	// Register internal API docs endpoint before starting the main subscription
	ns.registerApiDocsEndpoint()

	subscribe, err := ns.nc.QueueSubscribe(ns.basePath+".>", ns.queueName, func(msg *nats.Msg) {
		for _, endPoint := range ns.endPoints {
			if msg.Subject == ns.basePath {
				break
			}

			var matchSubject string
			if endPoint.pathHasWildcards {
				matchSubject = msg.Subject
			} else {
				if endPoint.pathSeparator == "/" {
					matchSubject = strings.Split(msg.Subject, endPoint.pathSeparator)[0]
				} else {
					matchSubject = ns.basePath + "." + strings.Split(strings.Replace(msg.Subject, ns.basePath, "", 1), endPoint.pathSeparator)[1]
				}
			}

			match, matchErr := endPoint.matchRegex.MatchString(matchSubject)
			if matchErr != nil {
				log.Printf("error matching regex: %v", matchErr)
				handleEndpointInternalException(msg, matchErr)
				return
			}
			if match {
				go ns.handleEndpointCall(endPoint, msg)
				return
			}
		}

		handleEndpointNotFound(msg)
	})

	if err != nil {
		return err
	}

	ns.subscription = subscribe

	err = ns.startChunkResponder()
	if err != nil {
		return err
	}

	// Register discovery endpoint (non-blocking, graceful degradation on failure)
	ns.registerDiscoveryEndpoint()

	// Register stats endpoint (broadcast, no queue group - all instances respond)
	ns.registerStatsEndpoint()

	return nil
}

func (ns *NatService) handleEndpointCall(endPoint *NatsEndpoint, msg *nats.Msg) {

	startTime := time.Now().UnixMicro()
	responseMsg := nats.Msg{}
	natsMessage, requestErr := ns.createNatsMessageFromRequest(endPoint, msg)
	if requestErr != nil {
		natError := NewValidationError("error parsing request", 400, requestErr)
		responseMsg.Header = nats.Header{}
		responseMsg.Header.Set(nats_service_common.STATUS, "400")
		jsonBA, jsonError := json.Marshal(natError)

		if jsonError != nil {
			errorMsg := fmt.Sprintf("error creating json from response: %v", jsonError)
			natsMessage.Logger.Print(errorMsg)
			responseMsg.Data = []byte(errorMsg)
		} else {
			responseMsg.Data = jsonBA
		}
		err := ns.respondToRequest(msg, &responseMsg)
		if err != nil {
			log.Printf("error responding to request: %v", err)
			return
		}
		return
	}

	err := endPoint.endPointFunc(natsMessage)
	endTime := time.Now().UnixMicro()
	elapsedTime := endTime - startTime

	var status string
	var responseMsgLog []byte

	if err != nil {

		//is it forwarded
		if err.Status == 302 {
			return
		}

		status = fmt.Sprintf("%d", err.Status)
		responseMsg.Header = nats.Header{}
		responseMsg.Header.Set(nats_service_common.STATUS, status)
		responseMsg.Header.Set(nats_service_common.MESSAGE_ID, natsMessage.MessageId)

		jsonBA, jsonError := json.Marshal(err)
		if jsonError != nil {
			errorMsg := fmt.Sprintf("error creating json from response: %v", jsonError)
			natsMessage.Logger.Print(errorMsg)
			responseMsg.Data = []byte(errorMsg)
		} else {
			responseMsg.Data = jsonBA
		}

		if len(responseMsg.Data) <= 1024 {
			responseMsgLog = responseMsg.Data
		} else {
			responseMsgLog = responseMsg.Data[:1024]
		}
	} else {
		if natsMessage.ResponseHeader != nil {
			responseMsg.Header = natsMessage.ResponseHeader
		} else {
			responseMsg.Header = nats.Header{}
		}

		//capture data for logging
		if len(natsMessage.ResponseBody) <= 1024 {
			responseMsgLog = natsMessage.ResponseBody
		} else {
			responseMsgLog = natsMessage.ResponseBody[:1024]
		}

		if len(natsMessage.ResponseBody) > ns.maxRespSizeToCompress {
			if ns.debug {
				natsMessage.Logger.Printf("response size %d is bigger than max size to compress %d,  compressing", len(natsMessage.ResponseBody), ns.maxRespSizeToCompress)
			}
			responseMsg.Header.Set(nats_service_common.COMPRESSED_HEADER, nats_service_common.GZIP_COMPRESSION_TYPE)
			natsMessage.ResponseBody = nats_service_common.GZipBytes(natsMessage.ResponseBody)
			if ns.debug {
				natsMessage.Logger.Printf("response size after compression %d", len(natsMessage.ResponseBody))
			}
		}
		responseMsg.Data = natsMessage.ResponseBody

		// Set the status header only if the handler function did not set it.
		// Although 200 is OK to indicate success, this allows the individual handlers to use other
		// more appropriate codes as needed - 201, 202, 204, etc.
		if responseMsg.Header.Get(nats_service_common.STATUS) == "" {
			responseMsg.Header.Set(nats_service_common.STATUS, "200")
		}
		responseMsg.Header.Set(nats_service_common.MESSAGE_ID, natsMessage.MessageId)
	}

	var reqMsgLog []byte
	if len(msg.Data) > 1024 {
		reqMsgLog = msg.Data[:1024]
	} else {
		reqMsgLog = msg.Data
	}

	errResponding := ns.respondToRequest(msg, &responseMsg)
	if errResponding != nil {
		natsMessage.Logger.Printf("error returning response: %v", errResponding)
	}

	// Track endpoint stats with status code for granular failure tracking
	stats := ns.getOrCreateEndpointStats(endPoint.path)
	var statusCode int
	if err != nil {
		statusCode = err.Status
	} else {
		statusCode = 200
	}
	stats.AddTransactionLatencyWithStatus(elapsedTime, statusCode)

	if ns.debug {
		natsMessage.Logger.Printf("apiStatus: %s, user:%s latency: %dμs, sub: %s, req:%s, resp: %s", status, natsMessage.UserId, elapsedTime, msg.Subject, reqMsgLog, responseMsgLog)
	}
}

func (ns *NatService) createNatsMessageFromRequest(endpoint *NatsEndpoint, msg *nats.Msg) (*NatsMessage, error) {
	var messageId string
	var userId string

	if msg.Header != nil && msg.Header.Get(nats_service_common.MESSAGE_ID) != "" {
		messageId = msg.Header.Get(nats_service_common.MESSAGE_ID)
		userId = msg.Header.Get(nats_service_common.USER_ID)
	} else {
		userId = ""
		messageId = uuid.New().String()
	}
	// Check if request data was chunked - reassemble before decompression
	if msg.Header != nil && msg.Header.Get(nats_service_common.CHUNKED_SUBJECT) != "" {
		chunkSubject := msg.Header.Get(nats_service_common.CHUNKED_SUBJECT)
		chunksId := msg.Header.Get(nats_service_common.CHUNKS_ID)
		chunksCount := msg.Header.Get(nats_service_common.CHUNKED_LENGTH)
		logger := createLogger(messageId)

		assembledData, err := ns.downloadRequestChunks(chunkSubject, messageId, chunksId, chunksCount, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to download request chunks: %w", err)
		}
		msg.Data = assembledData
	}

	if msg.Header != nil && msg.Header.Get(nats_service_common.COMPRESSED_HEADER) == nats_service_common.GZIP_COMPRESSION_TYPE {
		bytes, err := nats_service_common.GUnzipBytes(msg.Data)
		if err != nil {
			log.Printf("error unzipping message: %v", err)
			return nil, err
		}
		msg.Data = bytes
	}

	natsMessage := NatsMessage{
		Body:            msg.Data,
		Header:          msg.Header,
		Path:            msg.Subject,
		MessageId:       messageId,
		UserId:          userId,
		Logger:          createLogger(messageId),
		OriginalMessage: msg,
	}

	if endpoint.paramRegex != nil {
		var err error
		natsMessage.Parameters, err = extractParams(endpoint.paramRegex, msg.Subject, ns.basePath+"."+endpoint.path)
		if err != nil {
			return nil, err
		}
	} else {
		natsMessage.Parameters = make(map[string]string)
	}

	return &natsMessage, nil
}

func createLogger(messageId string) *log.Logger {
	return log.New(os.Stdout, messageId+" - ", log.Ltime|log.Ldate|log.Lshortfile|log.Lmsgprefix)
}

func handleEndpointNotFound(msg *nats.Msg) {
	responseMsg := nats.Msg{}
	responseMsg.Header = nats.Header{}

	responseMsg.Header.Set(nats_service_common.STATUS, "404")
	notFoundError := NewEndpointNotFoundError(msg.Subject)

	errorText, err := json.Marshal(notFoundError)

	if err != nil {
		errorText = []byte("unable to marshall EndpointNotFoundError")
		log.Printf("%s", errorText)
	}

	responseMsg.Data = errorText
	msg.RespondMsg(&responseMsg)
}
func handleEndpointInternalException(msg *nats.Msg, err error) {
	responseMsg := nats.Msg{}
	responseMsg.Header = nats.Header{}

	responseMsg.Header.Set(nats_service_common.STATUS, "500")
	notFoundError := NewServerError("Error while parsing request path", 500, err)

	errorText, err := json.Marshal(notFoundError)

	if err != nil {
		errorText = []byte("unable to marshall EndpointNotFoundError")
		log.Printf("%s", errorText)
	}

	responseMsg.Data = errorText
	msg.RespondMsg(&responseMsg)
}

func (ns *NatService) Shutdown() error {
	// Drain discovery subscription first (non-blocking if not registered)
	ns.drainDiscoverySubscription()

	// API docs endpoint is handled by the main subscription, no separate drain needed
	return ns.subscription.Drain()
}

func (ns *NatService) respondToRequest(req *nats.Msg, responseMsg *nats.Msg) error {

	//if it is small enough, then we send it back
	if len(responseMsg.Data) < ns.maxRespSizeToChunk {
		return req.RespondMsg(responseMsg)
	}

	//the data is too large, so we are going to chunk it
	chunkedData := nats_service_common.ChunkByteArray(responseMsg.Data, ns.maxRespSizeToChunk)

	responseMsg.Header.Set(nats_service_common.CHUNKED_SUBJECT, ns.chunkedSubscription.Subject)
	responseMsg.Header.Set(nats_service_common.CHUNKED_LENGTH, strconv.Itoa(len(chunkedData)))

	chunksId := uuid.New().String()
	responseMsg.Header.Set(nats_service_common.CHUNKS_ID, chunksId)
	ns.chunkCache.Set(chunksId, chunkedData, time.Minute*3)
	responseMsg.Data = []byte("")
	return req.RespondMsg(responseMsg)
}

func setupConnOptions(opts []nats.Option) []nats.Option {
	totalWait := 15 * time.Minute
	reconnectDelay := time.Second
	appId := os.Getenv("APPID")
	if appId == "" {
		appId = "unknownGoServiceConnection"
	}

	opts = append(opts, nats.Name(appId))
	opts = append(opts, nats.ReconnectWait(reconnectDelay))
	opts = append(opts, nats.MaxReconnects(int(totalWait/reconnectDelay)))
	opts = append(opts, nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
		log.Printf("%s nats.DisconnectErrHandler disconnected due to: %s, will attempt reconnects for %.0fm", time.Now(), err, totalWait.Minutes())
	}))
	opts = append(opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		log.Printf("%s nats.ReconnectHandler reconnected [%s]", time.Now(), nc.ConnectedUrl())
	}))
	opts = append(opts, nats.ClosedHandler(func(nc *nats.Conn) {
		log.Printf("%s nats.ClosedHandlerExiting: %v", time.Now(), nc.LastError())
		os.Exit(-1)
	}))

	return opts
}

func getEnvironmentVariableOrPanic(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Panicf("Environment variable %s is missing", key)
	}
	return value
}

func convertToRegex(input string, separator string) (*regexp2.Regexp, error) {
	// Escape the separator
	escapedSeparator := "\\" + separator
	input = strings.ReplaceAll(input, separator, escapedSeparator)

	regex := regexp.MustCompile(`:(\w+)`)
	result := regex.ReplaceAllStringFunc(input, func(s string) string {
		// Remove the leading ':' and wrap the name with the regex pattern
		return fmt.Sprintf(`(?P<%s>[^%s]+)`, strings.TrimPrefix(s, ":"), escapedSeparator)
	})

	// Add start and end identifiers to match the entire string
	result = "^" + result + "$"

	// Compile the regex string
	compiledRegex, err := regexp2.Compile(result, regexp2.RE2)
	if err != nil {
		return nil, err
	}

	return compiledRegex, nil
}

func extractParams(regex *regexp2.Regexp, subject, endPoint string) (map[string]string, error) {
	match, err := regex.FindStringMatch(subject)
	if err != nil {
		return nil, err
	}

	if match == nil {
		return nil, fmt.Errorf("Error parsing parameters.  endPoint expects %s", formatRequiredParams(regex, endPoint))
	}

	params := make(map[string]string)
	for _, name := range regex.GetGroupNames()[1:] {
		group := match.GroupByName(name)
		if group != nil {
			params[name] = group.String()
		}
	}

	return params, nil
}

func formatRequiredParams(regex *regexp2.Regexp, endPoint string) string {

	// Get the names of the groups
	groupNames := regex.GetGroupNames()

	// Create a slice to hold the parameter position messages
	var messages []string
	for i, name := range groupNames {
		if i != 0 { // Skip the 0 group, which is the entire match
			messages = append(messages, fmt.Sprintf("Parameter '%s' at position %d", name, i))
		}
	}

	// Join the messages into a single string
	example := "endPoint uri: " + endPoint

	message := strings.Join(messages, ". ")
	message = message + ". " + example

	return message
}
