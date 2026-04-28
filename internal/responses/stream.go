package responses

import "encoding/json"

type StreamCollector struct {
	currentEvent string
	itemsDone    []map[string]any
	carriers     map[string]Carrier
	completed    bool
	interrupted  bool
	finishReason string
}

func NewStreamCollector() *StreamCollector {
	return &StreamCollector{carriers: make(map[string]Carrier)}
}

func (c *StreamCollector) ObserveEventLine(line string) {
	if len(line) == 0 {
		return
	}
	if len(line) > 7 && line[:7] == "event: " {
		c.currentEvent = line[7:]
		return
	}
	if len(line) < 6 || line[:6] != "data: " {
		return
	}
	payload := line[6:]
	if payload == "[DONE]" {
		c.completed = true
		return
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(payload), &raw); err != nil {
		return
	}
	switch c.currentEvent {
	case "response.output_item.added", "response.output_item.done":
		item, _ := raw["item"].(map[string]any)
		if item == nil {
			return
		}
		if carrier := carrierFromItem(item); carrier.Kind != "" {
			c.carriers[carrierKey(carrier)] = carrier
		}
		if c.currentEvent == "response.output_item.done" {
			c.itemsDone = append(c.itemsDone, item)
		}
	case "response.completed":
		c.completed = true
		if response, _ := raw["response"].(map[string]any); response != nil {
			if status, _ := response["status"].(string); status != "" {
				c.finishReason = status
			}
		}
	}
}

func (c *StreamCollector) MarkInterrupted() { c.interrupted = true }
func (c *StreamCollector) Carriers() []Carrier {
	out := make([]Carrier, 0, len(c.carriers))
	for _, carrier := range c.carriers {
		out = append(out, carrier)
	}
	return out
}
func (c *StreamCollector) ResponseItems() []map[string]any { return c.itemsDone }
func (c *StreamCollector) StreamState() string {
	if c.interrupted {
		return "interrupted"
	}
	if c.completed {
		return "completed"
	}
	return "error"
}
func (c *StreamCollector) FinishReason() string { return c.finishReason }

func carrierFromItem(item map[string]any) Carrier {
	kind, _ := item["type"].(string)
	if kind != "reasoning" && kind != "compaction" {
		return Carrier{}
	}
	realID, _ := item["id"].(string)
	encrypted, _ := item["encrypted_content"].(string)
	if realID == "" && encrypted == "" {
		return Carrier{}
	}
	return Carrier{Kind: kind, RealID: realID, EncryptedContent: encrypted}
}

func carrierKey(c Carrier) string { return c.Kind + "|" + c.RealID + "|" + c.EncryptedContent }
