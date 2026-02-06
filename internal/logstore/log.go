package logstore

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

type Log struct{
	file 	*os.File
	offset	int64
}

// we need a constructor
func NewLog(path string) (*Log, error){
	file, err := os.OpenFile(
		path,
		os.O_CREATE | os.O_RDWR,
		0644,
	)
	if err != nil{
		return nil, err
	}
	l := &Log{
		file: file,
		offset: 0,
	}
	if err := l.recover(); err != nil{
		return nil, err
	}
	return l, nil
}

func encode(payload []byte)([]byte, error){
	length := int32(len(payload))
	crc := crc32.ChecksumIEEE(payload)

	buf := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], uint32(length))
	copy(buf[4:4+len(payload)], payload)
	binary.BigEndian.PutUint32(buf[4+len(payload):], crc)

	return buf, nil
}

func (l *Log) Append(data []byte) (offset int64, err error){
	offset = l.offset

	rec, err := encode(data)
	if err != nil{
		return 0,err
	}
	_,err = l.file.Write(rec)
	if err != nil{
		return 0, err
	}
	err = l.file.Sync()
	if err != nil{
		return 0, err
	}
	l.offset++
	return offset, nil
}

func (l *Log) ReadAt(pos int64)(data []byte, nextPos int64, err error){
	_, err = l.file.Seek(pos, io.SeekStart)
	if err != nil{
		return nil, 0, err
	}
	var lenBuf [4]byte
	if _, err := io.ReadFull(l.file, lenBuf[:]); err != nil{
		return nil, 0, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	payload := make([]byte, length)
	if _, err := io.ReadFull(l.file, payload); err != nil{
		return nil, 0, err
	}
	var crcBuf [4]byte
	if _, err := io.ReadFull(l.file, crcBuf[:]); err != nil{
		return nil, 0, err
	}
	expected := binary.BigEndian.Uint32(crcBuf[:])
	actual := crc32.ChecksumIEEE(payload)

	if actual != expected{
		return nil, 0, errors.New("crc mismatch")
	}
	nextPos = pos + 8 + int64(length)
	return payload, nextPos, nil
}

// recovery
func (l* Log) recover() error{
	var pos int64 = 0
	var offset int64 = 0

	for{
		_, err := l.file.Seek(pos, io.SeekStart)
		if err != nil{
			return err
		}
		var lenBuf [4]byte
		_, err = io.ReadFull(l.file, lenBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF{
			break
		}
		if err != nil{
			return err
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		next := pos + 8 + int64(length)

		_,err = l.file.Seek(next, io.SeekStart)
		if err != nil{
			break
		}
		pos = next
		offset++
	}
	if err:=l.file.Truncate(pos); err != nil{
		return err
	}
	l.offset = offset
	return  nil
}