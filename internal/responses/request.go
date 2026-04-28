package responses

import (
	"encoding/json"
	"fmt"
)

type Carrier struct {
	Kind             string
	RealID           string
	EncryptedContent string
}

type Request struct {
	Raw   map[string]any
	Input []map[string]any
}

func ParseRequest(body []byte) (Request, error) {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return Request{}, fmt.Errorf("decode body: %w", err)
	}
	items, _ := raw["input"].([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if ok {
			out = append(out, m)
		}
	}
	return Request{Raw: raw, Input: out}, nil
}

func ExtractRealCarriers(raw map[string]any) []Carrier {
	items, _ := raw["input"].([]any)
	carriers := make([]Carrier, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		kind, _ := m["type"].(string)
		if kind != "reasoning" && kind != "compaction" {
			continue
		}
		realID, _ := m["id"].(string)
		encrypted, _ := m["encrypted_content"].(string)
		if realID == "" && encrypted == "" {
			continue
		}
		carriers = append(carriers, Carrier{Kind: kind, RealID: realID, EncryptedContent: encrypted})
	}
	return carriers
}

func StripCarriers(raw map[string]any) (map[string]any, []string, int) {
	clone := deepClone(raw)
	items, _ := clone["input"].([]any)
	filtered := make([]any, 0, len(items))
	removedKinds := make([]string, 0)
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			filtered = append(filtered, item)
			continue
		}
		kind, _ := m["type"].(string)
		if kind == "reasoning" || kind == "compaction" {
			removedKinds = append(removedKinds, kind)
			continue
		}
		filtered = append(filtered, item)
	}
	clone["input"] = filtered
	return clone, removedKinds, len(removedKinds)
}

func MarshalCanonical(raw map[string]any) ([]byte, error) {
	return json.Marshal(raw)
}

func CarrierKinds(carriers []Carrier) []string {
	seen := make(map[string]struct{}, len(carriers))
	out := make([]string, 0, len(carriers))
	for _, carrier := range carriers {
		if _, ok := seen[carrier.Kind]; ok {
			continue
		}
		seen[carrier.Kind] = struct{}{}
		out = append(out, carrier.Kind)
	}
	return out
}

func deepClone(in map[string]any) map[string]any {
	body, _ := json.Marshal(in)
	var out map[string]any
	_ = json.Unmarshal(body, &out)
	return out
}
