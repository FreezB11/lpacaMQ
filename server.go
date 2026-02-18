package lpacamq

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Server struct {
	mq     *LpacaMQ
	mux    *http.ServeMux
	server *http.Server
}

func NewServer(mq *LpacaMQ, addr string) *Server {
	s := &Server{
		mq:  mq,
		mux: http.NewServeMux(),
	}
	
	s.routes()
	
	s.server = &http.Server{
		Addr:    addr,
		Handler: s.mux,
	}
	
	return s
}

func (s *Server) routes() {
	s.mux.HandleFunc("/publish", s.handlePublish)
	s.mux.HandleFunc("/subscribe/", s.handleSubscribe)
	s.mux.HandleFunc("/topics", s.handleListTopics)
	s.mux.HandleFunc("/stats", s.handleStats)
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	var req struct {
		Topic   string            `json:"topic"`
		Payload string            `json:"payload"`
		Headers map[string]string `json:"headers,omitempty"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	
	msg, err := s.mq.Publish(req.Topic, []byte(req.Payload))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"id":    msg.ID,
		"topic": msg.Topic,
	})
}

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	// Extract topic from URL: /subscribe/{topic}
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 3 {
		http.Error(w, "Topic required", http.StatusBadRequest)
		return
	}
	topic := parts[2]
	
	// Server-Sent Events for real-time streaming
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}
	
	queue, err := s.mq.Subscribe(topic)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	
	// Simple polling for demo (in production use WebSockets or proper SSE)
	for {
		msg, ok := queue.PopNonBlocking()
		if !ok {
			flusher.Flush()
			time.Sleep(100 * time.Millisecond)
			continue
		}
		
		data, _ := json.Marshal(msg)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
}

func (s *Server) handleListTopics(w http.ResponseWriter, r *http.Request) {
	topics := s.mq.ListTopics()
	json.NewEncoder(w).Encode(topics)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := map[string]interface{}{
		"topics": len(s.mq.ListTopics()),
	}
	json.NewEncoder(w).Encode(stats)
}

func (s *Server) Start() error {
	return s.server.ListenAndServe()
}

func (s *Server) Stop() error {
	return s.server.Close()
}