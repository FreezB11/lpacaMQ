package queue

import msg "lpacaMQ/internal/message"

type LpacaMQ struct{

}

func (mq *LpacaMQ) PublishInternal(message *msg.Message)(string, error){
	return "", nil // dummy 
}

func (mq *LpacaMQ) RemoveTransaction(txID string){
	
}