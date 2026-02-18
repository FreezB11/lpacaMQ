package lpacamq

import (
	"fmt"
	"log"
	"sync"
)

// LpacaMQ is the main message queue engine
type LpacaMQ struct {
	topics    map[string]*Topic
	consumers map[string]*Consumer
	mu        sync.RWMutex
}

// New creates a new LpacaMQ instance
func New() *LpacaMQ {
	return &LpacaMQ{
		topics:    make(map[string]*Topic),
		consumers: make(map[string]*Consumer),
	}
}

// CreateTopic creates a new topic explicitly
func (mq *LpacaMQ) CreateTopic(name string) error {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if _, exists := mq.topics[name]; exists {
		return fmt.Errorf("topic %s already exists", name)
	}

	mq.topics[name] = NewTopic(name)
	log.Printf("[LpacaMQ] Topic created: %s", name)
	return nil
}

// GetTopic gets an existing topic (returns error if not found)
func (mq *LpacaMQ) GetTopic(name string) (*Topic, error) {
	mq.mu.RLock()
	defer mq.mu.RUnlock()

	topic, exists := mq.topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found", name)
	}

	return topic, nil
}

// getOrCreateTopic gets existing topic or creates new one (internal use)
func (mq *LpacaMQ) getOrCreateTopic(name string) *Topic {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	if topic, exists := mq.topics[name]; exists {
		log.Printf("[LpacaMQ] Using existing topic: %s", name)
		return topic
	}

	// Auto-create
	topic := NewTopic(name)
	mq.topics[name] = topic
	log.Printf("[LpacaMQ] Auto-created topic: %s", name)
	return topic
}

// Publish publishes a message to a topic (auto-creates topic if needed)
func (mq *LpacaMQ) Publish(topicName string, payload []byte) (*Message, error) {
	if topicName == "" {
		return nil, fmt.Errorf("topic name cannot be empty")
	}

	topic := mq.getOrCreateTopic(topicName)

	msg := NewMessage(topicName, payload)
	if err := topic.Publish(msg); err != nil {
		return nil, err
	}

	return msg, nil
}

// Subscribe subscribes to a topic and returns the queue for consuming
// Auto-creates the topic if it doesn't exist
func (mq *LpacaMQ) Subscribe(topicName string) (*Queue, error) {
	if topicName == "" {
		return nil, fmt.Errorf("topic name cannot be empty")
	}

	topic := mq.getOrCreateTopic(topicName)
	return topic.Subscribe(), nil
}

// SubscribeWithHandler creates a consumer with a handler function
// This is the main method for consuming messages
func (mq *LpacaMQ) SubscribeWithHandler(topicName string, handler MessageHandler) (*Consumer, error) {
	if topicName == "" {
		return nil, fmt.Errorf("topic name cannot be empty")
	}

	if handler == nil {
		return nil, fmt.Errorf("handler cannot be nil")
	}

	// Get or create the topic first
	topic := mq.getOrCreateTopic(topicName)
	
	// Get the queue from the topic
	queue := topic.Subscribe()

	// Create consumer with the queue
	consumerID := generateID()
	consumer := NewConsumer(consumerID, topicName, handler, queue)

	// Store in consumers map
	mq.mu.Lock()
	mq.consumers[consumerID] = consumer
	mq.mu.Unlock()

	// Start the consumer
	consumer.Start()
	
	log.Printf("[LpacaMQ] Consumer %s subscribed to topic %s", consumerID, topicName)
	return consumer, nil
}

// Unsubscribe removes a consumer
func (mq *LpacaMQ) Unsubscribe(consumerID string) error {
	if consumerID == "" {
		return fmt.Errorf("consumer ID cannot be empty")
	}

	mq.mu.Lock()
	consumer, exists := mq.consumers[consumerID]
	delete(mq.consumers, consumerID)
	mq.mu.Unlock()

	if !exists {
		return fmt.Errorf("consumer %s not found", consumerID)
	}

	consumer.Stop()
	return nil
}

// DeleteTopic removes a topic
func (mq *LpacaMQ) DeleteTopic(name string) error {
	mq.mu.Lock()
	defer mq.mu.Unlock()

	topic, exists := mq.topics[name]
	if !exists {
		return fmt.Errorf("topic %s not found", name)
	}

	topic.Close()
	delete(mq.topics, name)
	log.Printf("[LpacaMQ] Topic deleted: %s", name)
	return nil
}

// ListTopics returns all topic names
func (mq *LpacaMQ) ListTopics() []string {
	mq.mu.RLock()
	defer mq.mu.RUnlock()

	names := make([]string, 0, len(mq.topics))
	for name := range mq.topics {
		names = append(names, name)
	}
	return names
}

// GetConsumerCount returns the number of active consumers
func (mq *LpacaMQ) GetConsumerCount() int {
	mq.mu.RLock()
	defer mq.mu.RUnlock()
	return len(mq.consumers)
}

// Close shuts down everything
func (mq *LpacaMQ) Close() {
	log.Println("[LpacaMQ] Shutting down...")

	// Stop all consumers first
	mq.mu.Lock()
	consumers := make([]*Consumer, 0, len(mq.consumers))
	for _, c := range mq.consumers {
		consumers = append(consumers, c)
	}
	mq.mu.Unlock()

	for _, c := range consumers {
		c.Stop()
	}

	// Close all topics
	mq.mu.Lock()
	topics := make([]*Topic, 0, len(mq.topics))
	for _, t := range mq.topics {
		topics = append(topics, t)
	}
	mq.mu.Unlock()

	for _, topic := range topics {
		topic.Close()
	}

	log.Println("[LpacaMQ] Shutdown complete")
}