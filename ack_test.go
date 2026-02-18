package lpacamq

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestAck(t *testing.T) {
	ackable := &AckableMessage{
		Message: NewMessage("test", []byte("data")),
		ackFunc: func() error {
			return nil
		},
	}

	err := ackable.Ack()
	if err != nil {
		t.Errorf("Ack failed: %v", err)
	}

	// Double ack should fail
	err = ackable.Ack()
	if err == nil {
		t.Error("Double ack should fail")
	}
}

func TestReliableConsumerRetry(t *testing.T) {
	queue := NewQueue()

	attempts := int32(0)
	handler := func(msg *AckableMessage) error {
		count := atomic.AddInt32(&attempts, 1)
		if count < 3 {
			return msg.Nack(true) // Requeue
		}
		return msg.Ack()
	}

	consumer := NewReliableConsumer("test", "test", handler, queue, 5)
	consumer.Start()

	msg := NewMessage("test", []byte("data"))
	queue.Push(msg)

	// Wait for retries (3 attempts with delays: 1s, 2s between)
	time.Sleep(3500 * time.Millisecond)

	if atomic.LoadInt32(&attempts) != 3 {
		t.Errorf("Expected 3 attempts, got %d", attempts)
	}

	consumer.Stop()
}

func TestReliableConsumerDLQ(t *testing.T) {
	queue := NewQueue()

	handler := func(msg *AckableMessage) error {
		return msg.Nack(true) // Always fail
	}

	consumer := NewReliableConsumer("test", "test", handler, queue, 2) // Max 2 retries
	consumer.Start()

	queue.Push(NewMessage("test", []byte("fail")))

	// Wait for processing + retries (2 retries with 1s, 2s delays)
	time.Sleep(3500 * time.Millisecond)
	consumer.Stop()

	// Should be in DLQ
	if consumer.dlq.Len() != 1 {
		t.Errorf("Expected 1 message in DLQ, got %d", consumer.dlq.Len())
	}
}