package KVstore

type configs struct {
	DirPath      string
	DataFileSize int64
	// whether it needs to persist per writing operation
	SyncWrites bool
}
