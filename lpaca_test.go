package lpacamq

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLpacaMQBasic(t *testing.T){
	mq := New()

	msg, err := mq.Publish("orders", []byte("order-1"))
	if err != nil {
		t.Fatalf(
			"Publish failed: %v",
			err,
		)
	}
	if msg.Topic != "orders" {
		t.Errorf("wrong topic: %s", msg.Topic)
	}
	topics := mq.ListTopics()
	if len(topics) != 1 || topics[0] != "orders" {
		t.Errorf("wrong topics list: %v", topics)
	}
	q, err := mq.Subscribe("orders")
	if err != nil{
		t.Fatalf("Subscribe failed: %v", err)
	}

	time.Sleep(10 * time.Millisecond)

	consumed, ok := q.PopNonBlocking()
	if !ok {
		t.Errorf("should have message in queue")
	}else if consumed.ID != msg.ID{
		t.Errorf("wrong message consumed")
	}
}

func TestLpacaMQMultipleTopics(t *testing.T) {
	mq := New()
	
	// Create multiple topics
	mq.Publish("orders", []byte("order"))
	mq.Publish("inventory", []byte("stock"))
	mq.Publish("payments", []byte("payment"))
	
	topics := mq.ListTopics()
	if len(topics) != 3 {
		t.Errorf("Expected 3 topics, got %d", len(topics))
	}
	
	// Each topic should be independent
	orderQueue, _ := mq.Subscribe("orders")
	invQueue, _ := mq.Subscribe("inventory")
	
	orderMsg, _ := orderQueue.PopNonBlocking()
	invMsg, _ := invQueue.PopNonBlocking()
	
	if string(orderMsg.Payload) != "order" {
		t.Error("Wrong order message")
	}
	if string(invMsg.Payload) != "stock" {
		t.Error("Wrong inventory message")
	}
}

func TestLpacaMQDeleteTopic(t *testing.T) {
	mq := New()
	mq.Publish("temp", []byte("data"))
	
	err := mq.DeleteTopic("temp")
	if err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	
	// Subscribe auto-creates the topic, so check ListTopics instead
	topics := mq.ListTopics()
	if len(topics) != 0 {
		t.Error("Topic should be deleted")
	}
	
	// Verify topic was actually deleted and is empty
	topic, err := mq.GetTopic("temp")
	if err == nil {
		// If GetTopic succeeds, the topic shouldn't have our old data
		if topic.Len() != 0 {
			t.Error("Deleted topic should be empty or not exist")
		}
	}
	// Note: Subscribe will auto-create, so we don't test that it errors
}

func TestLpacaMQConcurrency(t *testing.T) {
	mq := New()
	numProducers := 10
	msgsPerProducer := 100
	
	// Concurrent producers
	var wg sync.WaitGroup
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < msgsPerProducer; j++ {
				payload := fmt.Sprintf("producer-%d-msg-%d", id, j)
				mq.Publish("concurrent", []byte(payload))
			}
		}(i)
	}
	
	wg.Wait()
	
	// Verify count
	topic, _ := mq.GetTopic("concurrent")
	expected := numProducers * msgsPerProducer
	if topic.Len() != expected {
		t.Errorf("Expected %d messages, got %d", expected, topic.Len())
	}
}