package lpacamq

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServerPublish(t *testing.T) {
	mq := New()
	server := NewServer(mq, "localhost:0")
	
	// Test publish endpoint
	reqBody := `{"topic": "orders", "payload": "test-order"}`
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewBufferString(reqBody))
	req.Header.Set("Content-Type", "application/json")
	
	w := httptest.NewRecorder()
	server.handlePublish(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d: %s", w.Code, w.Body.String())
	}
	
	var resp map[string]string
	json.Unmarshal(w.Body.Bytes(), &resp)
	
	if resp["topic"] != "orders" {
		t.Errorf("Wrong topic in response: %s", resp["topic"])
	}
}

func TestServerListTopics(t *testing.T) {
	mq := New()
	mq.Publish("topic1", []byte("data"))
	mq.Publish("topic2", []byte("data"))
	
	server := NewServer(mq, "localhost:0")
	
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	w := httptest.NewRecorder()
	
	server.handleListTopics(w, req)
	
	var topics []string
	json.Unmarshal(w.Body.Bytes(), &topics)
	
	if len(topics) != 2 {
		t.Errorf("Expected 2 topics, got %d", len(topics))
	}
}