package lpacamq

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type DeliveryTag struct{
	Message *Message
	ConsumerID string
	DeliveredAt 	time.Time
	Acked bool
}

type AckableMessage struct {
	*Message
	ackFunc		func() error
	nackFunc	func(requeue bool) error
	acked		bool
	mu 			sync.Mutex
}

func (am *AckableMessage) Ack() error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if am.acked {
		return fmt.Errorf("message already acknowledged")
	}

	am.acked = true
	return am.ackFunc()
}

func (am *AckableMessage) Nack(requeue bool) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if am.acked {
		return fmt.Errorf("message already acknowledged")
	}

	am.acked = true
	return am.nackFunc(requeue)
}

type ReliableConsumer struct {
	*Consumer
	pendingAcks map[string]*AckableMessage
	maxRetries 	int
	retryDelay	time.Duration
	dlq 		*Queue // dead letter q
	mu 			sync.Mutex
}

func NewReliableConsumer(id, topic string, handler func(*AckableMessage) error, queue *Queue, maxRetries int) *ReliableConsumer {
	rc := &ReliableConsumer{
		maxRetries: maxRetries,
		retryDelay: time.Second,
		pendingAcks: make(map[string]*AckableMessage),
		dlq: NewQueue(),
	}

	wrapper := func(msg *Message) error {
		ackable := &AckableMessage{
			Message: msg,
			ackFunc: func() error {
				rc.mu.Lock()
				delete(rc.pendingAcks, msg.ID)
				rc.mu.Unlock()
				return nil
			},
			nackFunc: func(requeue bool) error {
				rc.handleNack(msg, requeue)
				return nil
			},
		}

		rc.mu.Lock()
		rc.pendingAcks[msg.ID] = ackable
		rc.mu.Unlock()

		return handler(ackable)
	}
	rc.Consumer = NewConsumer(id, topic, wrapper, queue)
	return rc
}

func (rc *ReliableConsumer) handleNack(msg *Message, requeue bool) {
	rc.mu.Lock()
	delete(rc.pendingAcks, msg.ID)
	rc.mu.Unlock()
	
	if !requeue || msg.RetryCount >= rc.maxRetries {
		// Send to dead letter queue
		log.Printf("[ReliableConsumer] Message %s exceeded retries, sending to DLQ", msg.ID)
		rc.dlq.Push(msg)
		return
	}
	
	// Retry with delay
	msg.RetryCount++
	time.Sleep(rc.retryDelay * time.Duration(msg.RetryCount))
	rc.Queue.Push(msg)
}