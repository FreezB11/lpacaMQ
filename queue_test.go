package lpacamq

import (
	"testing"
	"time"
)

func TestQueueBasic(t *testing.T){
	q := NewQueue()

	if len(q.messages) != 0 {
		t.Errorf("Expected empty queue, got %d messages", len(q.messages))
	}

	msg1 := NewMessage("topic1", []byte("payload1"))
	err := q.Push(msg1)

	if q.Len() != 1 {
		t.Errorf("Expected queue length 1, got %d", q.Len())
	}

	msg2, err := q.Pop()
	if err != nil {
		t.Errorf("Unexpected error popping message: %v", err)
	}

	if msg2.ID != msg1.ID {
		t.Errorf("Expected message ID %s, got %s", msg1.ID, msg2.ID)
	}

	if q.Len() != 0 {
		t.Errorf("Expected empty queue after pop, got %d messages", q.Len())
	}
}

func TestQueueBlocking(t *testing.T){
	q := NewQueue()
	_, ok := q.PopNonBlocking()
	if ok {
		t.Errorf("Expected PopNonBlocking to return false on empty queue")
	}

	done := make(chan *Message)
	go func() {
		msg, _ := q.Pop()
		done <- msg
	}()

	time.Sleep(10 * time.Millisecond)

	testMsg := NewMessage("topic2", []byte("payload2"))
	q.Push(testMsg)

	select {
	case msg := <-done:
		if msg.ID != testMsg.ID {
			t.Errorf("Expected message ID %s, got %s", testMsg.ID, msg.ID)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("Timed out waiting for message to be popped")
	}
}

func TestQueueConcurrent(t *testing.T){
	q := NewQueue()
	numMessages := 100

	go func(){
		for i := 0; i < numMessages; i++ {
			msg := NewMessage("topic", []byte("payload"))
			q.Push(msg)
		}
	}()

	received := 0
	done := make(chan bool)

	go func(){
		for{
			_, ok := q.PopNonBlocking()
			if !ok{
				if q.Len() == 0 {
					time.Sleep(10 * time.Millisecond)
					if q.Len() == 0 {
						break
					}
				}
				continue
			}
			received++
		}
		done <- true
	}()

	select{
	case <-done:
		if received != numMessages {
			t.Errorf("Expected to receive %d messages, got %d", numMessages, received)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("Timed out waiting for messages to be processed")
	}
}

func TestQueueClose(t *testing.T){
	q := NewQueue()
	go func(){
		time.Sleep(50 * time.Millisecond)
		q.Close()
	}()
	_, err := q.Pop()
	if err == nil {
		t.Errorf("Expected error when popping from closed queue")
	}

	err = q.Push(NewMessage("topic", []byte("payload")))
	if err == nil {
		t.Errorf("Expected error when pushing to closed queue")
	}
}