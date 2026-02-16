package internal

import (
	queue "lpacaMQ/internal/queue"
	utils "lpacaMQ/internal/utils"
	msg	  "lpacaMQ/internal/message"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Transaction struct{
	ID			string
	mq			*queue.LpacaMQ
	messages 	[]*msg.Message
	committed	bool
	aborted		bool
	mu 			sync.Mutex
}

func (tx *Transaction) Publish(topic string, payload []byte)(*msg.Message, error){
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return  nil, errors.New("transaction already completed")
	}

	msg := &msg.Message{
		ID: 		utils.GenerateID(),
		Topic: 		topic,
		Payload: 	payload,
		Timestamp: 	time.Now(),
		Metadata:  	map[string]interface{}{"tx_id":tx.ID},
		State: 		msg.StatePending,
		TraceID: 	utils.GenerateTraceID(),
	}
	tx.messages = append(tx.messages, msg)
	return msg, nil
}

func (tx *Transaction) Commit() error{
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return errors.New("transactions alread completed")
	}

	for _, msg := range tx.messages{
		if _, err := tx.mq.PublishInternal(msg); err != nil{
			tx.rollback()
			return fmt.Errorf("commit failed: %w", err)
		}
	}
	tx.committed = true
	tx.mq.RemoveTransaction(tx.ID)
	return nil
}

func (tx *Transaction) Abort() error{
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.committed || tx.aborted {
		return errors.New("transaction already completed")
	}
	tx.aborted = true
	tx.mq.RemoveTransaction(tx.ID)
	return nil
	}

func (tx *Transaction) rollback(){
	for _, m := range tx.messages{
		m.State = msg.StateRejected
	}
}