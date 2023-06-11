package data

import "KVstore/fio"

const FileSuffix = ".data"

type File struct {
	FileId      uint32
	WriteOffset int64 //store where to write next,only for active file
	IOManager   fio.IOManager
}

func OpenFile(dirPath string, fileId uint32) (*File, error) {
	return nil, nil
}
func (file *File) Write(data []byte) error {
	return nil
}
func (file *File) Sync() error {
	return nil
}
func (file *File) Read(offset int64) (*LogRecord, int64, error) {
	return nil, 0, nil
}
