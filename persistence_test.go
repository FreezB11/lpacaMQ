package lpacamq

import (
	"os"
	"testing"
)

func TestWALBasic(t *testing.T) {
	dir := "./test_wal"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	wal, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Write entries
	entry1 := &WALEntry{
		Operation: "PUBLISH",
		Topic:     "orders",
		Message:   NewMessage("orders", []byte("order-1")),
	}

	if err := wal.Write(entry1); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	entry2 := &WALEntry{
		Operation: "PUBLISH",
		Topic:     "orders",
		Message:   NewMessage("orders", []byte("order-2")),
	}

	if err := wal.Write(entry2); err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Close WAL
	if err := wal.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Recover with new WAL instance
	wal2, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	entries, err := wal2.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(entries) != 2 {
		t.Errorf("Expected 2 entries, got %d", len(entries))
	}

	// Check first entry
	if len(entries) > 0 {
		if string(entries[0].Message.Payload) != "order-1" {
			t.Errorf("Expected 'order-1', got '%s'", string(entries[0].Message.Payload))
		}
		if entries[0].Sequence != 1 {
			t.Errorf("Expected sequence 1, got %d", entries[0].Sequence)
		}
	}

	// Check second entry
	if len(entries) > 1 {
		if string(entries[1].Message.Payload) != "order-2" {
			t.Errorf("Expected 'order-2', got '%s'", string(entries[1].Message.Payload))
		}
		if entries[1].Sequence != 2 {
			t.Errorf("Expected sequence 2, got %d", entries[1].Sequence)
		}
	}
}

func TestWALRecoverEmpty(t *testing.T) {
	dir := "./test_wal_empty"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	// Create new WAL
	wal, err := NewWAL(dir)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	// Recover immediately (empty)
	entries, err := wal.Recover()
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("Expected 0 entries for empty WAL, got %d", len(entries))
	}

	wal.Close()
}

func TestWALCorruption(t *testing.T) {
	dir := "./test_wal_corrupt"
	os.RemoveAll(dir)
	defer os.RemoveAll(dir)

	wal, _ := NewWAL(dir)
	
	// Write valid entry
	wal.Write(&WALEntry{
		Operation: "PUBLISH",
		Topic:     "test",
		Message:   NewMessage("test", []byte("valid")),
	})
	
	wal.Close()

	// Corrupt the file by appending garbage
	f, _ := os.OpenFile(wal.path, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF}) // Invalid length prefix
	f.Close()

	// Should recover valid entry and skip corruption
	wal2, _ := NewWAL(dir)
	entries, err := wal2.Recover()
	
	// Should not error, should recover 1 valid entry
	if err != nil {
		t.Logf("Recover returned error (expected): %v", err)
	}
	
	if len(entries) != 1 {
		t.Errorf("Expected 1 valid entry recovered, got %d", len(entries))
	}

	wal2.Close()
}