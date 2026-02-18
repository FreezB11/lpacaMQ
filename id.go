package lpacamq

import (
	"crypto/rand"
	"encoding/hex"
	"sync/atomic"
	"time"
)

var (
	sequence uint32
	machineID string
)

func init() {
	// Generate random machine ID
	b := make([]byte, 3)
	rand.Read(b)
	machineID = hex.EncodeToString(b)
}

// GenerateID creates a unique ID (similar to MongoDB ObjectID)
// Format: 4 bytes timestamp + 3 bytes machine + 2 bytes pid + 3 bytes counter
func GenerateID() string {
	now := uint32(time.Now().Unix())
	seq := atomic.AddUint32(&sequence, 1)
	
	id := make([]byte, 12)
	
	// Timestamp (4 bytes)
	id[0] = byte(now >> 24)
	id[1] = byte(now >> 16)
	id[2] = byte(now >> 8)
	id[3] = byte(now)
	
	// Machine ID (3 bytes)
	copy(id[4:7], []byte(machineID))
	
	// Process ID (2 bytes) - simplified
	id[7] = 0
	id[8] = 1
	
	// Sequence (3 bytes)
	id[9] = byte(seq >> 16)
	id[10] = byte(seq >> 8)
	id[11] = byte(seq)
	
	return hex.EncodeToString(id)
}

// GenerateTraceID for distributed tracing
func GenerateTraceID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}