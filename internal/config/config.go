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
	SQLitePath            string
	DuckDBPath            string
	ActiveSessionWindow   time.Duration
	InactiveSessionRetain time.Duration
	DefaultReplayEnabled  bool
	DefaultProviderKind   string
}

func Load() (Config, error) {
	cfg := Config{
		ListenAddr:            getenv("LLM_GATEWAY_LISTEN_ADDR", ":8080"),
		LogJSON:               getenvBool("LLM_GATEWAY_LOG_JSON", true),
		SQLitePath:            getenv("LLM_GATEWAY_SQLITE_PATH", "var/llm-api-gateway.sqlite3"),
		DuckDBPath:            getenv("LLM_GATEWAY_DUCKDB_PATH", "var/llm-api-gateway.duckdb"),
		ActiveSessionWindow:   getenvDuration("LLM_GATEWAY_ACTIVE_WINDOW", 30*time.Minute),
		InactiveSessionRetain: getenvDuration("LLM_GATEWAY_SESSION_RETENTION", 14*24*time.Hour),
		DefaultReplayEnabled:  getenvBool("LLM_GATEWAY_REPLAY_ENABLED", true),
		DefaultProviderKind:   getenv("LLM_GATEWAY_PROVIDER_KIND", "copilot-api"),
	}
	if cfg.ActiveSessionWindow <= 0 {
		return Config{}, fmt.Errorf("active session window must be positive")
	}
	if cfg.InactiveSessionRetain < cfg.ActiveSessionWindow {
		return Config{}, fmt.Errorf("inactive session retention must be >= active session window")
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
