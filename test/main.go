package test

import "lpacaMQ/internal/broker"

func main() {
	b := broker.New()
	b.Start()
}