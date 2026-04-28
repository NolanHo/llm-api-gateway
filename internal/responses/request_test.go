package responses

import (
	"encoding/json"
	"testing"
)

func TestExtractRealCarriersAndStrip(t *testing.T) {
	raw := map[string]any{
		"input": []any{
			map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "hi"}}},
			map[string]any{"type": "reasoning", "summary": []any{map[string]any{"type": "summary_text", "text": "only summary"}}},
			map[string]any{"type": "reasoning", "id": "rs_1", "summary": []any{}, "encrypted_content": "enc-a"},
			map[string]any{"type": "compaction", "encrypted_content": "enc-b"},
		},
	}
	carriers := ExtractRealCarriers(raw)
	if len(carriers) != 2 {
		t.Fatalf("expected 2 real carriers, got %d", len(carriers))
	}
	if carriers[0].Kind != "reasoning" || carriers[1].Kind != "compaction" {
		t.Fatalf("unexpected carrier kinds: %#v", carriers)
	}
	stripped, removedKinds, removedCount := StripCarriers(raw)
	if removedCount != 3 {
		t.Fatalf("expected 3 removed carrier items, got %d", removedCount)
	}
	if len(removedKinds) != 3 {
		t.Fatalf("expected 3 removed carrier kinds, got %d", len(removedKinds))
	}
	items, _ := stripped["input"].([]any)
	if len(items) != 1 {
		t.Fatalf("expected only message item after strip, got %d items", len(items))
	}
}

func TestParseRequest(t *testing.T) {
	payload := map[string]any{"model": "gpt-5", "input": []any{map[string]any{"type": "message", "role": "user", "content": []any{map[string]any{"type": "input_text", "text": "hello"}}}}}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseRequest(body)
	if err != nil {
		t.Fatalf("parse request: %v", err)
	}
	if got := len(parsed.Input); got != 1 {
		t.Fatalf("expected 1 input item, got %d", got)
	}
}
