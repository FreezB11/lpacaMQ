package lpacamq

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestConsumerBasic(t *testing.T) {
	queue := NewQueue()

	var received int32
	handler := func(msg *Message) error {
		atomic.AddInt32(&received, 1)
		return nil
	}

	consumer := NewConsumer("test-consumer", "test", handler, queue)
	consumer.Start()

	// Push messages
	for i := 0; i < 5; i++ {
		queue.Push(NewMessage("test", []byte("hello")))
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&received) != 5 {
		t.Errorf("Expected 5 messages, got %d", received)
	}

	consumer.Stop()
}

func TestConsumerErrorHandling(t *testing.T) {
	queue := NewQueue()

	var errors int32
	handler := func(msg *Message) error {
		if string(msg.Payload) == "bad" {
			atomic.AddInt32(&errors, 1)
			return fmt.Errorf("processing error")
		}
		return nil
	}

	consumer := NewConsumer("test", "test", handler, queue)
	consumer.Start()

	queue.Push(NewMessage("test", []byte("good")))
	queue.Push(NewMessage("test", []byte("bad")))
	queue.Push(NewMessage("test", []byte("good")))

	time.Sleep(100 * time.Millisecond)

	if atomic.LoadInt32(&errors) != 1 {
		t.Errorf("Expected 1 error, got %d", errors)
	}

	consumer.Stop()
}

func TestConsumerStop(t *testing.T) {
	queue := NewQueue()

	var processed int32
	handler := func(msg *Message) error {
		time.Sleep(20 * time.Millisecond) // Slightly faster
		atomic.AddInt32(&processed, 1)
		return nil
	}

	consumer := NewConsumer("test", "test", handler, queue)
	consumer.Start()

	// Push some messages
	for i := 0; i < 5; i++ {
		queue.Push(NewMessage("test", []byte("data")))
	}

	// Give it time to start processing some
	time.Sleep(30 * time.Millisecond)

	// Stop - should complete quickly since we check stop signal frequently
	done := make(chan bool)
	go func() {
		consumer.Stop()
		done <- true
	}()

	select {
	case <-done:
		// Good - should complete quickly now
	case <-time.After(500 * time.Millisecond):
		t.Error("Stop took too long")
	}

	if consumer.IsActive() {
		t.Error("Consumer should not be active")
	}

	t.Logf("Processed %d messages before stop", atomic.LoadInt32(&processed))
}