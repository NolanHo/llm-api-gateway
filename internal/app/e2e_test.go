package app

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nolanho/llm-api-gateway/internal/config"
	"github.com/nolanho/llm-api-gateway/internal/logging"
	"github.com/nolanho/llm-api-gateway/internal/storage/sqlitestore"
)

type fakeDownstream struct {
	mu       sync.Mutex
	requests []map[string]any
	name     string
	host     string
	port     int
	server   *httptest.Server
}

func newFakeDownstream(t *testing.T, name string) *fakeDownstream {
	t.Helper()
	fd := &fakeDownstream{name: name}
	fd.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]any
		_ = json.NewDecoder(r.Body).Decode(&raw)
		fd.mu.Lock()
		fd.requests = append(fd.requests, raw)
		fd.mu.Unlock()
		if stream, _ := raw["stream"].(bool); stream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: response.output_item.added\n"))
			_, _ = w.Write([]byte("data: {\"item\":{\"type\":\"reasoning\",\"id\":\"rs_stream_" + name + "\",\"encrypted_content\":\"enc_stream_" + name + "\"}}\n\n"))
			_, _ = w.Write([]byte("event: response.output_item.done\n"))
			_, _ = w.Write([]byte("data: {\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"stream-ok\"}]}}\n\n"))
			_, _ = w.Write([]byte("event: response.completed\n"))
			_, _ = w.Write([]byte("data: {\"response\":{\"status\":\"completed\"}}\n\n"))
			_, _ = w.Write([]byte("data: [DONE]\n\n"))
			return
		}
		response := map[string]any{
			"output": []map[string]any{
				{"type": "reasoning", "id": "rs_" + name, "encrypted_content": "enc_" + name, "summary": []map[string]any{{"type": "summary_text", "text": "thinking"}}},
				{"type": "message", "role": "assistant", "content": []map[string]any{{"type": "output_text", "text": "ok-" + name}}},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	}))
	return fd
}

func (f *fakeDownstream) Close() { f.server.Close() }

func (f *fakeDownstream) Addr() (host string, port int) {
	return splitHostPort(f.server.URL)
}

func (f *fakeDownstream) Requests() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]any, len(f.requests))
	copy(out, f.requests)
	return out
}

func TestResponsesE2EAndAdmin(t *testing.T) {
	ctx := context.Background()
	a := newFakeDownstream(t, "a")
	defer a.Close()
	b := newFakeDownstream(t, "b")
	defer b.Close()
	cfg := testConfig(t, []sqlitestore.Account{
		accountFromDownstream("acc_a", a),
		accountFromDownstream("acc_b", b),
	})
	logger, err := logging.New(true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logger.Sync() }()
	app, err := New(ctx, cfg, logger)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = app.Close(ctx) }()
	gateway := httptest.NewServer(app.Handler())
	defer gateway.Close()

	firstReq := map[string]any{"model": "gpt-5.4-mini", "input": []map[string]any{{"type": "message", "role": "user", "content": []map[string]any{{"type": "input_text", "text": "hello"}}}}}
	firstBody := doJSON(t, gateway.Client(), gateway.URL+"/v1/responses", firstReq)
	if len(a.Requests()) != 1 {
		t.Fatalf("expected first request to go to account a, got a=%d b=%d", len(a.Requests()), len(b.Requests()))
	}
	respCarrier := firstBody["output"].([]any)[0].(map[string]any)
	strictReq := map[string]any{"model": "gpt-5.4-mini", "input": []any{
		map[string]any{"type": "message", "role": "user", "content": []map[string]any{{"type": "input_text", "text": "hello"}}},
		map[string]any{"type": "reasoning", "id": respCarrier["id"], "encrypted_content": respCarrier["encrypted_content"], "summary": respCarrier["summary"]},
		map[string]any{"type": "message", "role": "user", "content": []map[string]any{{"type": "input_text", "text": "followup"}}},
	}}
	_ = doJSON(t, gateway.Client(), gateway.URL+"/v1/responses", strictReq)
	if len(a.Requests()) != 2 {
		t.Fatalf("expected strict followup to route back to account a, got a=%d b=%d", len(a.Requests()), len(b.Requests()))
	}
	unknownCarrierReq := map[string]any{"model": "gpt-5.4-mini", "input": []any{
		map[string]any{"type": "message", "role": "user", "content": []map[string]any{{"type": "input_text", "text": "x"}}},
		map[string]any{"type": "reasoning", "id": "rs_unknown", "encrypted_content": "enc_unknown", "summary": []map[string]any{{"type": "summary_text", "text": "x"}}},
		map[string]any{"type": "message", "role": "user", "content": []map[string]any{{"type": "input_text", "text": "y"}}},
	}}
	_ = doJSON(t, gateway.Client(), gateway.URL+"/v1/responses", unknownCarrierReq)
	if len(b.Requests()) != 1 {
		t.Fatalf("expected replay fallback to choose account b, got a=%d b=%d", len(a.Requests()), len(b.Requests()))
	}
	lastReq := b.Requests()[0]
	for _, item := range lastReq["input"].([]any) {
		if m, ok := item.(map[string]any); ok {
			if m["type"] == "reasoning" || m["type"] == "compaction" {
				t.Fatalf("expected replay request to strip carriers, got %#v", lastReq)
			}
		}
	}

	accountsRes := struct {
		Accounts []map[string]any `json:"accounts"`
	}{}
	doInto(t, gateway.Client(), gateway.URL+"/admin/api/accounts", &accountsRes)
	if len(accountsRes.Accounts) < 2 {
		t.Fatalf("expected account overview rows, got %#v", accountsRes)
	}
	lineageID, _ := accountsRes.Accounts[0]["recent_turns"].([]any)
	_ = lineageID
	resp, err := gateway.Client().Get(gateway.URL + "/admin")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct == "" || resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected admin ui response: status=%d content-type=%q", resp.StatusCode, ct)
	}
}

func doJSON(t *testing.T, c *http.Client, url string, payload any) map[string]any {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := c.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var failure map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&failure)
		t.Fatalf("request failed: status=%d body=%v", resp.StatusCode, failure)
	}
	var raw map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		t.Fatal(err)
	}
	return raw
}

func doInto(t *testing.T, c *http.Client, url string, out any) {
	t.Helper()
	resp, err := c.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
}

func testConfig(t *testing.T, accounts []sqlitestore.Account) config.Config {
	t.Helper()
	dir := t.TempDir()
	accountsPath := filepath.Join(dir, "accounts.json")
	body, err := json.Marshal(accounts)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(accountsPath, body, 0o644); err != nil {
		t.Fatal(err)
	}
	return config.Config{
		ListenAddr: ":0", LogJSON: true, ServiceName: "test-gateway", OTELStdout: false,
		SQLitePath: filepath.Join(dir, "gateway.sqlite3"), DuckDBPath: filepath.Join(dir, "gateway.duckdb"), AccountsFile: accountsPath,
		CarrierHMACKey: "test-secret", UpstreamTimeout: 10 * time.Second, ActiveSessionWindow: 30 * time.Minute, InactiveSessionRetain: 14 * 24 * time.Hour,
		DefaultReplayEnabled: true, DefaultProviderKind: "copilot-api",
	}
}

func accountFromDownstream(id string, ds *fakeDownstream) sqlitestore.Account {
	host, port := ds.Addr()
	return sqlitestore.Account{AccountID: id, ProviderKind: "copilot-api", DisplayName: id, DownstreamHost: host, DownstreamPort: port, Enabled: true, State: "running"}
}

func splitHostPort(rawURL string) (string, int) {
	parts := strings.Split(strings.TrimPrefix(rawURL, "http://"), ":")
	p, _ := strconv.Atoi(parts[1])
	return parts[0], p
}
