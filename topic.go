package lpacamq

import (
	"fmt"
	"sync"
)

//represents named msg channel
type Topic struct{
	Name	string
	queue	*Queue
	mu		sync.RWMutex
	closed	bool
}

func NewTopic(name string) *Topic{
	return &Topic{
		Name: name,
		queue: NewQueue(),
	}
}

// adds msg to the topic
func (t *Topic) Publish(msg *Message) error{
	t.mu.RLock()
	if t.closed{
		t.mu.RUnlock()
		return fmt.Errorf("topic %s is closed", t.Name)
	}
	t.mu.RUnlock()

	return t.queue.Push(msg)
}

// subscribe return the internal q for consuming
func (t *Topic) Subscribe() *Queue{
	return t.queue
}

func (t *Topic) Len() int{
	return  t.queue.Len()
}

func (t *Topic) Close(){
	t.mu.Lock()
	defer t.mu.Unlock()

	t.closed = true
	t.queue.Close()
}