package lpacamq

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type WALEntry struct {
	Sequence uint64
	Timestamp int64
	Operation string // pub, ack; etc
	Topic string
	Message *Message
}

type WAL struct {
	file *os.File
	writer *bufio.Writer
	mu 	sync.Mutex
	seq uint64
	path string
}

func NewWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, "wal.log")
	file, err := os.OpenFile(path, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
	if err != nil {
		return  nil, err
	}

	return &WAL{
		file: file,
		writer: bufio.NewWriter(file),
		path: path,
	}, nil
}

func (w *WAL) Write(entry *WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	entry.Sequence = w.seq

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	length := uint32(len(data))
	if err := binary.Write(w.writer, binary.BigEndian, length); err != nil {
		return err
	}
	if _, err := w.writer.Write(data); err != nil{
		return err
	}
	return w.writer.Flush()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	
	w.writer.Flush()
	return w.file.Close()
}

func (w *WAL) Recover() ([]*WALEntry, error) {
	if w == nil {
		return nil, errors.New("WAL is nil")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	
	file, err := os.Open(w.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	
	var entries []*WALEntry
	reader := bufio.NewReader(file)
	
	for {
		// Read length
		var length uint32
		if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return entries, err
		}
		
		// Read data
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return entries, err
		}
		
		var entry WALEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			continue // Skip corrupted entry
		}
		
		entries = append(entries, &entry)
	}
	
	return entries, nil
}