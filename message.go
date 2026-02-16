package lpacamq

import (
	"time"
)

// single message rep
type Message struct {
	ID		string
	Topic	string
	Payload	[]byte
	Timestamp time.Time
}

// NewMessage creates a new message with the given topic and payload
func NewMessage(topic string, payload []byte) *Message {
	return &Message{
		ID:        generateID(),
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now(),
	}
}

var idCounter int64 = 0

// generateID generates a unique ID for the message
func generateID() string {
	idCounter++
	return time.Now().Format("20060102150405.000000000") + "-" + string(rune(idCounter))
}