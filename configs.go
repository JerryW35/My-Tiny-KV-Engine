package KVstore

import (
	"KVstore/index"
	"os"
)

type Configs struct {
	DirPath      string
	DataFileSize int64
	// whether it needs to do persistent on every write
	SyncWrites  bool
	IndexerType index.IndexType
}
type IteratorConfigs struct {
	Reverse bool
	Prefix  []byte
}

var DefaultConfigs = Configs{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, //256MB
	SyncWrites:   false,
	IndexerType:  index.Btree,
}
var DefaultIteratorConfigs = IteratorConfigs{
	Reverse: false,
	Prefix:  nil,
}
