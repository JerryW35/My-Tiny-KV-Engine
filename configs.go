package KVstore

import (
	"KVstore/index"
)

type Configs struct {
	DirPath      string
	DataFileSize int64
	// whether it needs to do persistent on every write
	SyncWrites     bool
	IndexerType    index.IndexType
	IndexerDirPath string
}
type IteratorConfigs struct {
	Reverse bool
	Prefix  []byte
}
type WriteBatchConfigs struct {
	MaxBatchNum uint
	SyncWrites  bool //whether do persistence when commits
}

var DefaultConfigs = Configs{
	DirPath:      "./",
	DataFileSize: 256 * 1024 * 1024, //256MB
	SyncWrites:   false,
	IndexerType:  index.Btree,
}
var DefaultIteratorConfigs = IteratorConfigs{
	Reverse: false,
	Prefix:  nil,
}
var DefaultWriteBatchConfigs = WriteBatchConfigs{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
