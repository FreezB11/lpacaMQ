package lpacamq

//a basic in memo queue

import (
	"errors"
	"sync"
)

// q is a simple thread safe fifo q for msgs
type Queue struct {
	messages 	[]*Message
	mu 			sync.Mutex
	cond		*sync.Cond
	closed		bool
}

// NewQueue creates a new empty queue
func NewQueue() *Queue {
	q := &Queue{
		messages: make([]*Message, 0),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// push adds a message to the q
func (q *Queue) Push(msg *Message) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return errors.New("queue is closed")
	}

	q.messages = append(q.messages, msg)
	q.cond.Signal() // signal waiting goroutines
	return nil
}

// pop removes and returns the next message from the q, blocking if empty
func (q *Queue) Pop() (*Message, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.messages) == 0 && !q.closed {
		q.cond.Wait() // wait for a message to be pushed
	}

	if len(q.messages) == 0 && q.closed {
		return nil, errors.New("queue is closed")
	}

	msg := q.messages[0]
	q.messages = q.messages[1:]
	return msg, nil
}

// popnonBlocking tries to get a msg without blocking
func (q *Queue) PopNonBlocking() (*Message, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.messages) == 0 {
		return nil, false
	}

	msg := q.messages[0]
	q.messages = q.messages[1:]
	return msg, true
}

// len returns the number of messages in the q
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.messages)
}

// close shuts the q
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.cond.Broadcast() // wake all waiting goroutines
}