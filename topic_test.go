package lpacamq

import (
	"fmt"
	"testing"
	// "time"

)

func TestTopicBasic(t *testing.T){
	topic := NewTopic("orders")

	//test publish
	msg := NewMessage("orders", []byte("order-123"))
	err := topic.Publish(msg)
	if err != nil{
		t.Errorf("Publish failed: %v", err)
	}
	if topic.Len() != 1{
		t.Errorf("expected length 1, got %d",topic.Len())
	}

	queue := topic.Subscribe()
	consumed, ok := queue.PopNonBlocking()
	if !ok{
		t.Errorf("failed to consume msg")
	}
	if consumed.ID != msg.ID{
		t.Error("consumed wrong msg")
	}
}

func TestTopicMultipleMessages(t *testing.T){
	topic := NewTopic("events")
	numMsgs := 10

	for i := 0; i < numMsgs; i++{
		msg := NewMessage("events",[]byte(fmt.Sprintf("event-%d", i)))
		topic.Publish(msg)
	}

	if topic.Len() != numMsgs{
		t.Errorf("expected %d msgs, got %d", numMsgs, topic.Len())
	}

	//consume all
	queue := topic.Subscribe()
	for i := 0; i < numMsgs; i++{
		msg, ok := queue.PopNonBlocking()
		if!ok{
			t.Fatalf("failed to get msg %d",i)
		}
		expected := fmt.Sprintf("event-%d", i)
		if string(msg.Payload) != expected{
			t.Errorf("Expected %s, got %s", expected, string(msg.Payload))
		}
	}
	if topic.Len() != 0 {
		t.Errorf("Expected empty topic, got %d", topic.Len())
	}
}

func TestTopicClose(t *testing.T) {
	topic := NewTopic("test")
	topic.Publish(NewMessage("test", []byte("data")))
	
	topic.Close()
	
	err := topic.Publish(NewMessage("test", []byte("more")))
	if err == nil {
		t.Error("Expected error publishing to closed topic")
	}
}