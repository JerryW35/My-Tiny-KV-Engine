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
	// sync when write how many bytes, 0 means no sync
	BytesPerSync uint
	MMapLoad     bool
	//threshold of merge
	DataFileMergeRatio float32
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
	DirPath:            "./",
	IndexerDirPath:     "./",
	DataFileSize:       256 * 1024 * 1024, //256MB
	SyncWrites:         false,
	IndexerType:        index.Btree,
	BytesPerSync:       0,
	MMapLoad:           false, //whether use mmap to load data file
	DataFileMergeRatio: 0.5,
}
var DefaultIteratorConfigs = IteratorConfigs{
	Reverse: false,
	Prefix:  nil,
}
var DefaultWriteBatchConfigs = WriteBatchConfigs{
	MaxBatchNum: 10000,
	SyncWrites:  true,
}
