package message

import (
	"sync"
	"time"
)

type MessagePriority int
type DeliveryMode int
type MessageState int

// we need something to give the message priority
// and the delivery mode 
// also we need to know the state of message
const (
	PriorityLow MessagePriority = iota
	PriorityNormal
	PriorityHigh
	PriorityCritical
)

const (
	DeliveryModeTransient DeliveryMode = iota // memory only
	DeliveryModePersistent	// disk + memory
	DeliveryModeReplicated // multiple node
)

const (
	StatePending MessageState = iota
	StateDelivered
	StateAcknowledged
	StateRejected
	StateExpired
	StateDeadLetter
)

type Message struct{
	ID				string 					`json:"id"`
	Topic			string					`json:"topic"`
	Payload			[]byte					`json:"payload"`
	Headers			map[string]string		`json:"headers"`
	Priority		MessagePriority			`json:"priority"`
	Timestamp		time.Time				`json:"timestamp"`
	DeliveryMode	DeliveryMode			`json:"delivery_mode"`
	State 			MessageState			`json:"state"`
	RetryCount		int 					`json:"retry_count"`
	MaxRetries		int 					`json:"max_retries"`
	Delay			time.Duration			`json:"-"`
	ExpiresAt		*time.Time				`json:"expires_at,omitempty"`
	ContentType		string					`json:"content_type"`
	Metadata		map[string]interface{}	`json:"metadata"`
	SequenceNum		uint64					`json:"sequence_num"`
	PartitionKey 	string					`json:"partition_key"`
	ConsumerGroup 	string                 	`json:"consumer_group,omitempty"`
	AckDeadline   	*time.Time             	`json:"ack_deadline,omitempty"`
	TraceID       	string                 	`json:"trace_id"`
	ParentID      	string                 	`json:"parent_id,omitempty"`
	mu            	sync.RWMutex           	`json:"-"`
}

func (m *Message) Copy() *Message {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	copy := &Message{
		ID:            m.ID,
		Topic:         m.Topic,
		Payload:       append([]byte{}, m.Payload...),
		Headers:       make(map[string]string),
		Priority:      m.Priority,
		Timestamp:     m.Timestamp,
		DeliveryMode:  m.DeliveryMode,
		State:         m.State,
		RetryCount:    m.RetryCount,
		MaxRetries:    m.MaxRetries,
		Delay:         m.Delay,
		ContentType:   m.ContentType,
		Metadata:      make(map[string]interface{}),
		SequenceNum:   m.SequenceNum,
		PartitionKey:  m.PartitionKey,
		ConsumerGroup: m.ConsumerGroup,
		TraceID:       m.TraceID,
		ParentID:      m.ParentID,
	}
	
	for k, v := range m.Headers {
		copy.Headers[k] = v
	}
	for k, v := range m.Metadata {
		copy.Metadata[k] = v
	}
	
	if m.ExpiresAt != nil {
		t := *m.ExpiresAt
		copy.ExpiresAt = &t
	}
	if m.AckDeadline != nil {
		t := *m.AckDeadline
		copy.AckDeadline = &t
	}
	
	return copy
}