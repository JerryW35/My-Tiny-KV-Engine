package data

type RecordType int

const (
	PUT RecordType = iota
	DELETE
)

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

// EncodeLogRecord encode the log record and return bytes and its length
func EncodeLogRecord(record *LogRecord) ([]byte, int64) {
	return nil, 0
}