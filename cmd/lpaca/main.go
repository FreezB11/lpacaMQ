package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	
	lpaca "lpacaMQ"
)

func main() {
	var (
		httpAddr = flag.String("http", ":8080", "HTTP API address")
		dataDir  = flag.String("data", "./data", "Data directory")
	)
	flag.Parse()
	
	log.Println("Starting LpacaMQ...")
	
	// Create message queue
	mq := lpaca.New()
	
	// Setup persistence (optional)
	wal, err := lpaca.NewWAL(*dataDir)
	if err != nil {
		log.Printf("WAL not available: %v", err)
	} else {
		defer wal.Close()
		// Recover if needed
	}
	
	// Create some example consumers
	consumer1, err := mq.SubscribeWithHandler("orders", func(msg *lpaca.Message) error {
		log.Printf("[Consumer-1] Processing order: %s", string(msg.Payload))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Unsubscribe(consumer1.ID)
	
	consumer2, err := mq.SubscribeWithHandler("notifications", func(msg *lpaca.Message) error {
		log.Printf("[Consumer-2] Sending notification: %s", string(msg.Payload))
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	defer mq.Unsubscribe(consumer2.ID)
	
	// Start HTTP API
	server := lpaca.NewServer(mq, *httpAddr)
	go func() {
		log.Printf("HTTP API listening on %s", *httpAddr)
		if err := server.Start(); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()
	
	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	log.Println("Shutting down...")
	
	server.Stop()
	mq.Close()
	log.Println("Goodbye!")
}