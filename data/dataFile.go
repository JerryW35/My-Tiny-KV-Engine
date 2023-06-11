package data

import (
	"KVstore/fio"
	"fmt"
	"io"
	"path/filepath"
)

const FileSuffix = ".data"

type File struct {
	FileId      uint32
	WriteOffset int64 //store where to write next,only for active file
	IOManager   fio.IOManager
}

func OpenFile(dirPath string, fileId uint32) (*File, error) {
	fileName := filepath.Join(dirPath + fmt.Sprintf("%9d", fileId) + FileSuffix)
	fio, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &File{
		FileId:      fileId,
		WriteOffset: 0,
		IOManager:   fio,
	}, nil
}
func (file *File) Write(data []byte) error {
	return nil
}
func (file *File) Sync() error {
	return nil
}

// Read logRecord from data file
func (file *File) Read(offset int64) (*LogRecord, int64, error) {
	//get header
	headerBuf, err := file.readNBytes(maxLogRecordHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := DecodeLogRecord(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.CRC == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	//get the key+value
	var recordSize = int64(header.KeySize) + int64(header.ValueSize) + headerSize

	return nil, 0, nil
}
func (file *File) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = file.IOManager.Read(b, offset)
	return b, nil
}
