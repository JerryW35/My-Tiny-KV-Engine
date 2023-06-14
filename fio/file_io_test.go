package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func destroyFile(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestFileIO_Write(t *testing.T) {
	curFile, err := os.Getwd()
	fio, err := NewFileIOManager(filepath.Join(curFile, "test.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte(""))
	assert.Equal(t, 0, n)
	assert.Nil(t, err)

	n, err = fio.Write([]byte("123"))
	assert.Equal(t, 3, n)
	t.Log(n, err)
	destroyFile(filepath.Join(curFile, "test.data"))
}
func TestFileIO_Read(t *testing.T) {
	curFile, err := os.Getwd()
	fio, err := NewFileIOManager(filepath.Join(curFile, "test.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("key-a"))
	assert.Equal(t, 5, n)
	t.Log(n, err)

	bytes := make([]byte, 5)
	n, err = fio.Read(bytes, 0)
	assert.Equal(t, 5, n)
	t.Log(n, err)
	t.Log(string(bytes))
	destroyFile(filepath.Join(curFile, "test.data"))
}
func TestFileIO_Sync(t *testing.T) {
	curFile, err := os.Getwd()
	fio, err := NewFileIOManager(filepath.Join(curFile, "test.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Sync()
	assert.Nil(t, err)
	destroyFile(filepath.Join(curFile, "test.data"))
}
func TestFileIO_Close(t *testing.T) {
	curFile, err := os.Getwd()
	fio, err := NewFileIOManager(filepath.Join(curFile, "test.data"))
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	err = fio.Close()
	assert.Nil(t, err)
	destroyFile(filepath.Join(curFile, "test.data"))
}
