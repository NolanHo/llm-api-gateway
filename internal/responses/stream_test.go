package responses

import "testing"

func TestStreamCollectorCapturesCarrierOnAdded(t *testing.T) {
	collector := NewStreamCollector()
	collector.ObserveEventLine("event: response.output_item.added")
	collector.ObserveEventLine(`data: {"item":{"type":"reasoning","id":"rs_1","encrypted_content":"enc_1"}}`)
	collector.ObserveEventLine("event: response.output_item.done")
	collector.ObserveEventLine(`data: {"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}}`)
	carriers := collector.Carriers()
	if len(carriers) != 1 {
		t.Fatalf("expected 1 carrier, got %d", len(carriers))
	}
	if carriers[0].RealID != "rs_1" {
		t.Fatalf("unexpected carrier id: %#v", carriers[0])
	}
	if got := len(collector.ResponseItems()); got != 1 {
		t.Fatalf("expected 1 completed output item, got %d", got)
	}
}
