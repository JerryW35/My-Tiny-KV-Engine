package data

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type RecordType = byte

const (
	PUT RecordType = iota
	DELETE
)

// crc type keySize valueSize
// 4 + 1 + 5 + 5
const maxLogRecordHeaderSize = 15

// use for kv dir, to get the location of the data
type LogRecordPos struct {
	Fid    uint32 // file id, represent which file the data is in
	Offset int64
}
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  RecordType
}
type logRecordHeader struct {
	CRC       uint32
	Type      RecordType
	KeySize   uint32
	ValueSize uint32
}

// EncodeLogRecord encode the log record and return bytes and its length
// crc type keySize valueSize || key value
// 4   + 1   + 5      + 5
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	//init header
	header := make([]byte, maxLogRecordHeaderSize)

	//type
	header[4] = record.Type
	var index = 5
	//key and value in header
	index += binary.PutVarint(header[index:], int64(len(record.Key)))
	index += binary.PutVarint(header[index:], int64(len(record.Value)))
	size := index + len(record.Key) + len(record.Value)

	encBytes := make([]byte, size)
	copy(encBytes[:index], header[:index])
	//copy key and value
	copy(encBytes[index:], record.Key)
	copy(encBytes[index+len(record.Key):], record.Value)
	//compute crc
	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)
	fmt.Printf("header length: %d ,crc :%d", index, crc)

	return encBytes, int64(size)
}
func DecodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}
	header := logRecordHeader{
		CRC:  binary.LittleEndian.Uint32(buf[:4]),
		Type: buf[4],
	}
	var index = 5
	//get key size and value size
	keySize, n := binary.Varint(buf[index:])
	header.KeySize = uint32(keySize)
	index += n

	valueSize, n := binary.Varint(buf[index:])
	header.ValueSize = uint32(valueSize)
	index += n

	return &header, int64(index)
}
func getCRC(log *LogRecord, header []byte) uint32 {
	if log == nil {
		return 0
	}
	crc := crc32.ChecksumIEEE(header[:])
	crc = crc32.Update(crc, crc32.IEEETable, log.Key)
	crc = crc32.Update(crc, crc32.IEEETable, log.Value)
	return crc
}
