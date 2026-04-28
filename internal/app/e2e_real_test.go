package app

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nolanho/llm-api-gateway/internal/config"
	"github.com/nolanho/llm-api-gateway/internal/logging"
	"github.com/nolanho/llm-api-gateway/internal/storage/sqlitestore"
	"net/http"
	"net/http/httptest"
)

func TestRealDownstreamE2E(t *testing.T) {
	if os.Getenv("LLM_GATEWAY_REAL_E2E") != "1" {
		t.Skip("set LLM_GATEWAY_REAL_E2E=1 to run")
	}
	apiKey := os.Getenv("LLM_GATEWAY_REAL_API_KEY")
	accountsJSON := os.Getenv("LLM_GATEWAY_REAL_ACCOUNTS_JSON")
	if apiKey == "" || accountsJSON == "" {
		t.Skip("missing LLM_GATEWAY_REAL_API_KEY or LLM_GATEWAY_REAL_ACCOUNTS_JSON")
	}
	var accounts []sqlitestore.Account
	if err := json.Unmarshal([]byte(accountsJSON), &accounts); err != nil {
		t.Fatalf("decode accounts json: %v", err)
	}
	dir := t.TempDir()
	accountsPath := filepath.Join(dir, "accounts.json")
	if err := os.WriteFile(accountsPath, []byte(accountsJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{ListenAddr: ":0", LogJSON: true, ServiceName: "real-e2e", OTELStdout: false, SQLitePath: filepath.Join(dir, "gateway.sqlite3"), DuckDBPath: filepath.Join(dir, "gateway.duckdb"), AccountsFile: accountsPath, CarrierHMACKey: "test-secret", UpstreamTimeout: 90 * time.Second, ActiveSessionWindow: 30 * time.Minute, InactiveSessionRetain: 14 * 24 * time.Hour, DefaultReplayEnabled: true, DefaultProviderKind: "copilot-api"}
	logger, err := logging.New(true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logger.Sync() }()
	app, err := New(context.Background(), cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close(context.Background()) }()
	gateway := httptest.NewServer(app.Handler())
	defer gateway.Close()
	body, _ := json.Marshal(map[string]any{"model": "gpt-5.4-mini", "input": []map[string]any{{"type": "message", "role": "user", "content": []map[string]any{{"type": "input_text", "text": "Reply with the word ok only."}}}}})
	req, err := http.NewRequest("POST", gateway.URL+"/v1/responses", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := gateway.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		var failure map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&failure)
		t.Fatalf("unexpected status=%d body=%v", resp.StatusCode, failure)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["output"]; !ok {
		t.Fatalf("missing output in real response: %v", raw)
	}
}
