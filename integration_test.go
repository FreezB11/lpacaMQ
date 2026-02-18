package lpacamq

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestIntegrationBasic(t *testing.T) {
	mq := New()
	defer mq.Close()
	
	var received int32
	
	// Subscribe before publishing
	consumer, err := mq.SubscribeWithHandler("orders", func(msg *Message) error {
		atomic.AddInt32(&received, 1)
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	
	// Publish some messages
	for i := 0; i < 10; i++ {
		_, err := mq.Publish("orders", []byte("order-data"))
		if err != nil {
			t.Fatalf("Publish failed: %v", err)
		}
	}
	
	// Wait for processing
	time.Sleep(200 * time.Millisecond)
	
	if atomic.LoadInt32(&received) != 10 {
		t.Errorf("Expected 10 messages, got %d", received)
	}
	
	// Unsubscribe
	err = mq.Unsubscribe(consumer.ID)
	if err != nil {
		t.Errorf("Unsubscribe failed: %v", err)
	}
}

func TestIntegrationMultipleConsumers(t *testing.T) {
	mq := New()
	defer mq.Close()
	
	var count1, count2 int32
	
	// Two consumers on same topic
	_, err := mq.SubscribeWithHandler("events", func(msg *Message) error {
		atomic.AddInt32(&count1, 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	
	_, err = mq.SubscribeWithHandler("events", func(msg *Message) error {
		atomic.AddInt32(&count2, 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	
	// Publish messages
	for i := 0; i < 20; i++ {
		mq.Publish("events", []byte("event"))
	}
	
	time.Sleep(200 * time.Millisecond)
	
	total := atomic.LoadInt32(&count1) + atomic.LoadInt32(&count2)
	if total != 20 {
		t.Errorf("Expected 20 total, got %d (c1: %d, c2: %d)", total, count1, count2)
	}
}

func TestIntegrationGracefulShutdown(t *testing.T) {
	mq := New()
	
	var processed int32
	
	consumer, _ := mq.SubscribeWithHandler("test", func(msg *Message) error {
		time.Sleep(20 * time.Millisecond)
		atomic.AddInt32(&processed, 1)
		return nil
	})
	
	// Queue up messages
	for i := 0; i < 50; i++ {
		mq.Publish("test", []byte("data"))
	}
	
	// Give time for some processing
	time.Sleep(50 * time.Millisecond)
	
	// Shutdown
	mq.Close()
	
	// Consumer should be stopped
	if consumer.IsActive() {
		t.Error("Consumer should be stopped after Close()")
	}
	
	// Some messages should have been processed
	processedCount := atomic.LoadInt32(&processed)
	if processedCount == 0 {
		t.Error("Should have processed some messages")
	}
	t.Logf("Processed %d messages before shutdown", processedCount)
}