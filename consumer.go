package lpacamq

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// MessageHandler processes messages
type MessageHandler func(msg *Message) error

// Consumer represents a message consumer
type Consumer struct {
	ID       string
	Topic    string
	Handler  MessageHandler
	Queue    *Queue

	active   int32 // atomic
	stopChan chan struct{}
	wg       sync.WaitGroup
	mu       sync.Mutex // protects stopChan close
}

// NewConsumer creates a consumer
func NewConsumer(id, topic string, handler MessageHandler, queue *Queue) *Consumer {
	return &Consumer{
		ID:       id,
		Topic:    topic,
		Handler:  handler,
		Queue:    queue,
		stopChan: make(chan struct{}),
	}
}

// Start begins consuming messages in a goroutine
func (c *Consumer) Start() {
	if !atomic.CompareAndSwapInt32(&c.active, 0, 1) {
		return // Already started
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		log.Printf("[Consumer %s] Started on topic %s", c.ID, c.Topic)

		for {
			// Check if we should stop (non-blocking)
			select {
			case <-c.stopChan:
				log.Printf("[Consumer %s] Received stop signal", c.ID)
				return
			default:
			}

			// Try non-blocking pop first
			msg, ok := c.Queue.PopNonBlocking()
			if !ok {
				// No message available, wait a bit and retry
				select {
				case <-c.stopChan:
					return
				case <-time.After(5 * time.Millisecond):
					continue
				}
			}

			// Process the message
			if err := c.Handler(msg); err != nil {
				log.Printf("[Consumer %s] Error processing message %s: %v", c.ID, msg.ID, err)
			}
		}
	}()
}

// Stop gracefully stops the consumer
func (c *Consumer) Stop() {
	// Only stop if active
	if atomic.CompareAndSwapInt32(&c.active, 1, 0) {
		c.mu.Lock()
		select {
		case <-c.stopChan:
			// Already closed
		default:
			close(c.stopChan)
		}
		c.mu.Unlock()

		c.wg.Wait()
		log.Printf("[Consumer %s] Stopped", c.ID)
	}
}

// IsActive returns true if consumer is running
func (c *Consumer) IsActive() bool {
	return atomic.LoadInt32(&c.active) == 1
}