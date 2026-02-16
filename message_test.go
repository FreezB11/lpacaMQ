package lpacamq

import (
	"testing"
	"fmt"
)

func TestNewMessage(t *testing.T) {
	msg := NewMessage("test-topic", []byte("hello world"))
	if msg.Topic != "test-topic" {
		t.Errorf("Expected topic 'test-topic', got '%s'", msg.Topic)
	}
	if string(msg.Payload) != "hello world" {
		t.Errorf("Expected payload 'hello world', got '%s'", string(msg.Payload))
	}
	if msg.ID == "" {
		t.Error("Expected non-empty ID")
	}
	fmt.Printf("Message ID: %s, Timestamp: %s\n", msg.ID, msg.Timestamp)

	msg2 := NewMessage("test-topic", []byte("hello again"))
	if msg2.ID == msg.ID {
		t.Error("Expected unique IDs for different messages")
	}
}

func TestMessageStructure(t *testing.T) {
	payload := []byte(`{"key": "value", "number": 123}`)
	msg := NewMessage("json-topic", payload)

	if msg.Topic != "json-topic" {
		t.Errorf("Expected topic 'json-topic', got '%s'", msg.Topic)
	}
	if string(msg.Payload) != string(payload) {
		t.Errorf("Expected payload '%s', got '%s'", string(payload), string(msg.Payload))
	}
	if msg.ID == "" {
		t.Error("Expected non-empty ID")
	}
	fmt.Printf("Message ID: %s, Timestamp: %s\n", msg.ID, msg.Timestamp)
}