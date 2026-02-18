package lpacamq

import (
	"testing"
)

func TestGenerateID(t *testing.T) {
	// Test uniqueness
	ids := make(map[string]bool)
	for i := 0; i < 10000; i++ {
		id := GenerateID()
		if ids[id] {
			t.Fatalf("Duplicate ID generated: %s", id)
		}
		ids[id] = true
		
		// Check format (24 hex chars)
		if len(id) != 24 {
			t.Errorf("Wrong ID length: %d", len(id))
		}
	}
}

func TestGenerateTraceID(t *testing.T) {
	trace := GenerateTraceID()
	if len(trace) != 32 {
		t.Errorf("Wrong trace ID length: %d", len(trace))
	}
}