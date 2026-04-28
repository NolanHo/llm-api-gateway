package responses

import (
	"fmt"
	"strings"

	"github.com/nolanho/llm-api-gateway/internal/storage/duckstore"
)

func RequestItems(raw map[string]any) []map[string]any {
	items, _ := raw["input"].([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if ok {
			out = append(out, m)
		}
	}
	return out
}

func ResponseItems(raw map[string]any) []map[string]any {
	items, _ := raw["output"].([]any)
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if ok {
			out = append(out, m)
		}
	}
	return out
}

func FlattenItems(turnPK, turnID, lineageSessionID string, requestItems, responseItems []map[string]any) []duckstore.TurnItem {
	items := make([]duckstore.TurnItem, 0, len(requestItems)+len(responseItems))
	seq := 0
	for _, item := range requestItems {
		seq++
		items = append(items, flattenOne(turnPK, turnID, lineageSessionID, seq, "request", item))
	}
	for _, item := range responseItems {
		seq++
		items = append(items, flattenOne(turnPK, turnID, lineageSessionID, seq, "response", item))
	}
	return items
}

func EffectiveConversationText(requestItems, responseItems []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(requestItems)+len(responseItems))
	appendTexts := func(items []map[string]any, phase string) {
		for _, item := range items {
			role, kind, text := summarizeItem(item)
			if text == "" {
				continue
			}
			out = append(out, map[string]any{"phase": phase, "role": role, "item_kind": kind, "text": text})
		}
	}
	appendTexts(requestItems, "request")
	appendTexts(responseItems, "response")
	return out
}

func flattenOne(turnPK, turnID, lineageSessionID string, seq int, phase string, item map[string]any) duckstore.TurnItem {
	role, kind, text := summarizeItem(item)
	toolName, toolArgs, toolResult := summarizeToolFields(item)
	return duckstore.TurnItem{
		TurnPK:           turnPK,
		TurnID:           turnID,
		LineageSessionID: lineageSessionID,
		ItemSeq:          seq,
		Phase:            phase,
		Role:             role,
		ItemKind:         kind,
		TextContent:      text,
		ToolName:         toolName,
		ToolArgs:         toolArgs,
		ToolResult:       toolResult,
		ItemJSON:         item,
	}
}

func summarizeItem(item map[string]any) (role string, itemKind string, text string) {
	itemType, _ := item["type"].(string)
	if itemType == "message" {
		role, _ = item["role"].(string)
		itemKind = "text"
		text = flattenContentText(item["content"])
		return role, itemKind, text
	}
	switch itemType {
	case "function_call":
		return "assistant", "tool_call", ""
	case "function_call_output":
		return "tool", "tool_result", flattenContentText(item["output"])
	case "compaction", "reasoning":
		return "assistant", "other", ""
	default:
		return role, ifEmpty(itemType, "other"), flattenContentText(item["content"])
	}
}

func summarizeToolFields(item map[string]any) (string, any, any) {
	switch item["type"] {
	case "function_call":
		name, _ := item["name"].(string)
		return name, item["arguments"], nil
	case "function_call_output":
		return "", nil, item["output"]
	default:
		return "", nil, nil
	}
}

func flattenContentText(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []any:
		parts := make([]string, 0, len(x))
		for _, part := range x {
			switch p := part.(type) {
			case string:
				parts = append(parts, p)
			case map[string]any:
				if text, _ := p["text"].(string); text != "" {
					parts = append(parts, text)
				}
				if text, _ := p["output_text"].(string); text != "" {
					parts = append(parts, text)
				}
			}
		}
		return strings.TrimSpace(strings.Join(parts, "\n"))
	default:
		return ""
	}
}

func ifEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func EnsureMapArray(v any) []map[string]any {
	if v == nil {
		return nil
	}
	arr, ok := v.([]map[string]any)
	if ok {
		return arr
	}
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if ok {
			out = append(out, m)
		}
	}
	return out
}

func MustItems(raw []map[string]any) []map[string]any {
	if raw == nil {
		return []map[string]any{}
	}
	return raw
}

func DebugItemsLabel(items []map[string]any) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("%v", item["type"]))
	}
	return strings.Join(parts, ",")
}
