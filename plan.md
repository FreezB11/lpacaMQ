lpacaMQ — Week-by-Week Development Plan
Scope: single-node first
Delivery: at-least-once
Architecture: log-based, append-only
Week 1 — Durable log foundation (MOST IMPORTANT)
Goal
Persist messages safely and read them back after restart.
If this week is weak, everything later collapses.
Tasks
Define on-disk record format
Implement append-only log
Implement sparse index
Write unit tests for crash recovery
Deliverables
logstore/log.go
logstore/segment.go
logstore/index.go
Can:
Append messages
Restart process
Read by offset
Acceptance test
1. Write 10k messages
2. Kill process
3. Restart
4. Read all messages correctly
Week 2 — Partition runtime & concurrency model
Goal
Make concurrent producers safe without locks everywhere.
Tasks
Implement partition writer goroutine
Single-writer per partition rule
Bounded input channel
Reader by offset
Deliverables
partition/writer.go
partition/reader.go
Stress test with concurrent producers
Acceptance test
100 goroutines produce messages
No data corruption
Offsets strictly increasing
Week 3 — Basic networking & protocol
Goal
Producers and consumers can talk to the broker.
Tasks
Define binary protocol (produce / fetch)
Implement TCP server
Connection lifecycle
Basic framing
Deliverables
network/protocol.go
network/server.go
Simple client (Go test or CLI)
Acceptance test
Client produces message
Another client fetches it
Week 4 — Consumer offsets & ACKs
Goal
Support failure-safe consumption.
Tasks
Consumer identity
Offset commit API
Persist offsets
Redelivery on failure
Deliverables
consumer/consumer.go
consumer/offset.go
Acceptance test
Consumer reads messages
Crashes before ACK
Restart → messages reappear
Week 5 — Topics, routing & partitions
Goal
Support multiple topics and scale writes.
Tasks
Topic abstraction
Partition routing (hash / round-robin)
Multiple partitions per topic
Deliverables
topic/topic.go
topic/router.go
Acceptance test
Topic with 3 partitions
Messages distributed
Ordering preserved per partition
Week 6 — Retention & cleanup
Goal
Prevent infinite disk growth.
Tasks
Segment rolling
Time-based retention
Size-based retention
Safe deletion logic
Deliverables
logstore/retention.go
Acceptance test
Old segments deleted
Active readers unaffected
Week 7 — Back-pressure & batching
Goal
Stay stable under load.
Tasks
Producer throttling
Fetch batching
Slow consumer handling
Deliverables
Batching logic in network layer
Configurable limits
Acceptance test
Slow consumer does not crash broker
Producer receives back-pressure
Week 8 — Observability & tooling
Goal
Know what your MQ is doing.
Tasks
Queue depth metrics
Consumer lag metrics
Basic CLI tool
Deliverables
metrics/metrics.go
lpacactl
Acceptance test
CLI shows:
- topics
- partitions
- consumer lag
Week 9 — Hardening & fault testing
Goal
Break it on purpose.
Tasks
Chaos tests
Kill -9 tests
Disk full simulation
Slow disk simulation
Deliverables
test/chaos/
Bug fixes
Acceptance test
Broker recovers from crashes
No corrupted logs
Week 10 — (Optional) Replication MVP
Goal
Learn distributed systems pain.
Tasks
Leader/follower replication
Commit index
Manual failover
Deliverables
replication/leader.go
replication/follower.go
Acceptance test
Follower catches up
Leader crash → follower promoted