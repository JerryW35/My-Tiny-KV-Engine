package KVstore

import "KVstore/index"

type Configs struct {
	DirPath      string
	DataFileSize int64
	// whether it needs to persist per writing operation
	SyncWrites  bool
	IndexerType index.IndexType
}
