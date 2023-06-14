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

var DefaultConfigs = Configs{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 1024, //256MB
	SyncWrites:   false,
	IndexerType:  index.Btree,
}
