package data

import (
	"KVstore/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const FileSuffix = ".data"

var (
	ErrorCRC = errors.New("the crc is wrong ")
)

type File struct {
	FileId      uint32
	WriteOffset int64 //store where to write next,only for active file
	IOManager   fio.IOManager
}

func OpenFile(dirPath string, fileId uint32) (*File, error) {
	fileName := filepath.Join(dirPath + fmt.Sprintf("%09d", fileId) + FileSuffix)
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
	n, err := file.IOManager.Write(data)
	if err != nil {
		return err
	}
	file.WriteOffset += int64(n)
	return nil

}
func (file *File) Sync() error {
	return file.IOManager.Sync()
}
func (file *File) Close() error {
	return file.IOManager.Close()
}

// Read logRecord from data file
// return logRecord, logRecord.size, err
func (file *File) Read(offset int64) (*LogRecord, int64, error) {
	//here solve the corner case:
	//if we read the last record in the file which is DELETE Type,
	//and the size of record is less than maxLogRecordHeaderSize
	//we cannot read the record correctly.
	//e.g. our record:12 bytes. will  EOF error
	fileSize, err := file.IOManager.Size()
	if err != nil {
		return nil, 0, err
	}
	var headerBytes int64 = maxLogRecordHeaderSize
	if offset+headerBytes > fileSize {
		headerBytes = fileSize - offset
	}
	//get header
	headerBuf, err := file.readNBytes(headerBytes, offset)
	if err != nil {
		return nil, 0, err
	}
	header, headerSize := DecodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.CRC == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	logRecord := LogRecord{Type: header.Type}
	//get the size we need to read
	keySize, valueSize := int64(header.KeySize), int64(header.ValueSize)
	var recordSize = keySize + valueSize + headerSize
	// read the data from data file
	if keySize > 0 || valueSize > 0 {
		kvBuf, err := file.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}
	//check the crc
	crc := getCRC(&logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.CRC {
		return nil, 0, ErrorCRC
	}
	return &logRecord, recordSize, nil
}
func (file *File) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = file.IOManager.Read(b, offset)
	return b, nil
}
