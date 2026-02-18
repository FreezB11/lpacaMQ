package lpacamq

import "time"

type Config struct {
	MaxQueueDepth      int
	DefaultMaxRetries  int
	RetryDelay         time.Duration
	AutoCreateTopics   bool
	ConsumerBufferSize int
}

func DefaultConfig() *Config {
	return &Config{
		MaxQueueDepth:      10000,
		DefaultMaxRetries:  3,
		RetryDelay:         time.Second * 5,
		AutoCreateTopics:   true,
		ConsumerBufferSize: 100,
	}
}