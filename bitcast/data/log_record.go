package data

// use for kv dir, to get the location of the data
type LogRecordPos struct {
	FId    uint32 // file id, represent which file the data is in
	Offset int64
}
