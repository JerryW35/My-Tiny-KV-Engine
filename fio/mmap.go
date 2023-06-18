package fio

import (
	"golang.org/x/exp/mmap"
	"os"
)

type MMapIO struct {
	readerAt *mmap.ReaderAt
}

func NewMMapIOManager(fileName string) (*MMapIO, error) {
	_, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm)
	if err != nil {
		return nil, err
	}
	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMapIO{readerAt}, nil
}

func (mmap *MMapIO) Read(bytes []byte, i int64) (int, error) {
	return mmap.readerAt.ReadAt(bytes, i)
}

func (mmap *MMapIO) Write(bytes []byte) (int, error) {
	return 0, nil
}

func (mmap *MMapIO) Sync() error {
	return nil
}

func (mmap *MMapIO) Close() error {
	return mmap.readerAt.Close()
}

func (mmap *MMapIO) Size() (int64, error) {
	return int64(mmap.readerAt.Len()), nil
}
