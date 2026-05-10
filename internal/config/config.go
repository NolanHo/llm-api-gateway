package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	ListenAddr            string
	LogJSON               bool
	ServiceName           string
	OTELStdout            bool
	SQLitePath            string
	DuckDBPath            string
	AccountsFile          string
	CarrierHMACKey        string
	UpstreamTimeout       time.Duration
	ActiveSessionWindow   time.Duration
	InactiveSessionRetain time.Duration
	DefaultReplayEnabled  bool
	DefaultProviderKind   string
	AccessToken           string
	RetryMaxAttempts      int
	RetryBackoff          time.Duration
	StrictReplayRetry     bool
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:            getenv("LLM_GATEWAY_LISTEN_ADDR", ":8080"),
		LogJSON:               getenvBool("LLM_GATEWAY_LOG_JSON", true),
		ServiceName:           getenv("LLM_GATEWAY_SERVICE_NAME", "llm-api-gateway"),
		OTELStdout:            getenvBool("LLM_GATEWAY_OTEL_STDOUT", false),
		SQLitePath:            getenv("LLM_GATEWAY_SQLITE_PATH", "var/llm-api-gateway.sqlite3"),
		DuckDBPath:            getenv("LLM_GATEWAY_DUCKDB_PATH", "var/llm-api-gateway.duckdb"),
		AccountsFile:          getenv("LLM_GATEWAY_ACCOUNTS_FILE", ""),
		CarrierHMACKey:        getenv("LLM_GATEWAY_CARRIER_HMAC_KEY", "dev-only-unsafe-key"),
		UpstreamTimeout:       getenvDuration("LLM_GATEWAY_UPSTREAM_TIMEOUT", 5*time.Minute),
		ActiveSessionWindow:   getenvDuration("LLM_GATEWAY_ACTIVE_WINDOW", 30*time.Minute),
		InactiveSessionRetain: getenvDuration("LLM_GATEWAY_SESSION_RETENTION", 14*24*time.Hour),
		DefaultReplayEnabled:  getenvBool("LLM_GATEWAY_REPLAY_ENABLED", true),
		DefaultProviderKind:   getenv("LLM_GATEWAY_PROVIDER_KIND", "copilot-api"),
		AccessToken:           getenv("LLM_GATEWAY_ACCESS_TOKEN", ""),
		RetryMaxAttempts:      getenvInt("LLM_GATEWAY_RETRY_MAX_ATTEMPTS", 8),
		RetryBackoff:          getenvDuration("LLM_GATEWAY_RETRY_BACKOFF", 200*time.Millisecond),
		StrictReplayRetry:     getenvBool("LLM_GATEWAY_STRICT_REPLAY_RETRY", false),
	}
	if cfg.ActiveSessionWindow <= 0 {
		return Config{}, fmt.Errorf("active session window must be positive")
	}
	if cfg.InactiveSessionRetain < cfg.ActiveSessionWindow {
		return Config{}, fmt.Errorf("inactive session retention must be >= active session window")
	}
	if cfg.CarrierHMACKey == "" {
		return Config{}, fmt.Errorf("carrier hmac key must not be empty")
	}
	if cfg.UpstreamTimeout <= 0 {
		return Config{}, fmt.Errorf("upstream timeout must be positive")
	}
	if cfg.RetryMaxAttempts < 1 {
		return Config{}, fmt.Errorf("retry max attempts must be >= 1")
	}
	if cfg.RetryBackoff < 0 {
		return Config{}, fmt.Errorf("retry backoff must be >= 0")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return fallback
}

func getenvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		i, err := strconv.Atoi(v)
		if err == nil {
			return i
		}
	}
	return fallback
}
