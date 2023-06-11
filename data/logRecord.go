package data

type RecordType int

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
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}
func DecodeLogRecord(buf []byte) (*logRecordHeader, int64) {
	return nil, 0
}
