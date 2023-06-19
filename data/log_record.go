package data

import (
	"encoding/binary"
	"hash/crc32"
)

type RecordType = byte

const (
	PUT RecordType = iota
	DELETE
	COMMIT
)

// crc type keySize valueSize
// 4 + 1 + 5 + 5
const maxLogRecordHeaderSize = 15

// use for kv dir, to get the location of the data
type LogRecordPos struct {
	Fid    uint32 // file id, represent which file the data is in
	Offset int64
	Size   uint32
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
type TxnRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
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
	//fmt.Printf("header length: %d ,crc :%d", index, crc)

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

// Encode LogRecordPos
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen64+binary.MaxVarintLen32*2)
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)
	index += binary.PutVarint(buf[index:], int64(pos.Size))
	return buf[:index]
}

// Decode LogRecordPos
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	fileId, n1 := binary.Varint(buf[0:])
	offset, n2 := binary.Varint(buf[n1:])
	size, _ := binary.Varint(buf[n1+n2:])
	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
		Size:   uint32(size),
	}
}
