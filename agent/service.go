package agent

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	_ "time/tzdata" // embed IANA tz db; alpine runtime ships no system tzdata

	nats_service "github.com/transactrx/nats-service/pkg/nats-service"

	"github.com/transactrx/ai-agent-go-service/pkg/aws/s3files"
	"github.com/transactrx/ai-agent-go-service/pkg/config"
	"github.com/transactrx/ai-agent-go-service/pkg/promptadmin"
	"github.com/transactrx/ai-agent-go-service/pkg/promptstore"
	"github.com/transactrx/ai-agent-go-service/pkg/workflow/engine"
	"github.com/transactrx/ai-agent-go-service/pkg/workflowlist"
)

// Run boots the agent service and blocks until SIGINT/SIGTERM, then shuts down.
// It encapsulates the full bootstrap: config, NATS, hosts (s3files, promptstore),
// registry + defaults + overrides, engine load, optional ListWorkflows / prompt-admin
// endpoints, NATS start, graceful shutdown.
func (s *Service) Run(ctx context.Context) error {
	cfg := config.Load()
	logger := s.logger
	if logger == nil {
		logger = log.New(os.Stdout, fmt.Sprintf("[%s] ai-agent-service ", cfg.Region), log.LstdFlags|log.Lshortfile)
	}

	logger.Print("boot: initializing nats-service")
	natservice, err := nats_service.NewLowLevel(
		cfg.NatsBasePath, cfg.NatsQueueName, cfg.NatsURL, cfg.NatsJWT, cfg.NatsKey,
		1024*2, 1024*300,
	)
	if err != nil {
		return fmt.Errorf("nats init: %w", err)
	}
	natservice.SetDescription("Flexible workflow engine over NATS. Workflows registered as endpoints.")
	natservice.SetRepositoryURL("https://github.com/transactrx/ai-agent-go-service")

	hosts := map[string]any{
		"nats":          natservice,
		"nats_basePath": cfg.NatsBasePath,
	}

	bucket := resolveFilesBucket(logger)
	uploader, err := s3files.New(ctx, bucket)
	if err != nil {
		return fmt.Errorf("s3files init: %w (set S3_FILES_BUCKET and AWS creds, or APP_NAME+ENVIRONMENT)", err)
	}
	hosts["s3files"] = s3files.Uploader(uploader)
	logger.Printf("boot: s3files uploader configured (bucket=%s)", uploader.Bucket())

	promptTable := strings.TrimSpace(os.Getenv("DYNAMODB_PROMPTS_TABLE"))
	if promptTable == "" {
		promptTable = "opensearchaichatapi-assistant-prompts"
	}
	promptRegion := strings.TrimSpace(os.Getenv("AWS_REGION_DYNAMODB"))
	if promptRegion == "" {
		promptRegion = "us-east-1"
	}
	prompts, perr := promptstore.New(ctx, promptTable, promptRegion)
	if perr != nil {
		logger.Printf("boot: promptstore disabled (init failed): %v", perr)
	} else if perr := prompts.Provision(ctx, logger); perr != nil {
		logger.Printf("boot: promptstore disabled (provision failed): %v", perr)
	} else {
		hosts["promptstore"] = prompts
		logger.Printf("boot: promptstore configured (table=%s)", promptTable)
	}

	for k, h := range s.extraHosts {
		hosts[k] = h
	}

	registry, err := s.buildRegistry()
	if err != nil {
		return fmt.Errorf("registry: %w", err)
	}

	eng, err := engine.New(engine.Config{
		Source:   engine.NewFilesystemSource(s.workflowsDir),
		Registry: registry,
		Hosts:    hosts,
		Logger:   logger,
	})
	if err != nil {
		return fmt.Errorf("engine: %w", err)
	}

	if err := eng.LoadAll(ctx); err != nil {
		return fmt.Errorf("workflow load: %w", err)
	}
	loaded := eng.WorkflowIDs()
	if len(loaded) == 0 {
		return fmt.Errorf("workflow load: no workflows registered (see prior log lines)")
	}
	logger.Printf("workflows registered: %v", loaded)

	if err := workflowlist.Register(workflowlist.Deps{
		Engine:   eng,
		NatsHost: natservice,
		Logger:   logger,
	}); err != nil {
		logger.Printf("boot: workflowlist disabled: %v", err)
	} else {
		logger.Print("boot: workflowlist endpoint registered (ListWorkflows)")
	}

	if p, ok := hosts["promptstore"].(*promptstore.Store); ok {
		if err := promptadmin.Register(promptadmin.Deps{
			Engine:    eng,
			Store:     p,
			NatsHost:  natservice,
			NatsConn:  natservice.GetNatsService(),
			BasePath:  cfg.NatsBasePath,
			Logger:    logger,
			LookupEnv: os.LookupEnv,
		}); err != nil {
			logger.Printf("boot: promptadmin disabled: %v", err)
		} else {
			logger.Print("boot: promptadmin endpoints registered (PromptGet/PromptSave/PromptHistory)")
		}
	}

	if err := natservice.Start(); err != nil {
		return fmt.Errorf("nats start: %w", err)
	}
	logger.Print("service started")

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Print("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = eng.Shutdown(shutdownCtx)
	_ = natservice.Shutdown()
	return nil
}
