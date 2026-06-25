package config

import (
	"log"
	"os"
)

// Config holds boot-time settings. Per-workflow secrets and DSNs are NOT
// here — they're resolved on demand by NodeEnv.Secret at provider Init.
type Config struct {
	NatsURL       string
	NatsJWT       string
	NatsKey       string
	NatsQueueName string
	NatsBasePath  string

	Region string
}

// Load reads the cycle-1 env vars.
func Load() *Config {
	return &Config{
		NatsURL:       getEnvOrPanic("NATS_URL"),
		NatsJWT:       getEnvOrDefault("NATS_JWT", ""),
		NatsKey:       getEnvOrDefault("NATS_KEY", ""),
		NatsQueueName: getEnvOrPanic("NATS_QUEUE_NAME"),
		NatsBasePath:  getEnvOrPanic("NATS_BASE_PATH"),
		Region:        getEnvOrDefault("AWS_REGION", "unknown"),
	}
}

func getEnvOrPanic(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Panicf("environment variable %s is missing", key)
	}
	return v
}

func getEnvOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
