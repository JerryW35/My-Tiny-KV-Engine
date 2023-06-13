package data

import (
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"testing"
)

func TestEncodeLogRecord(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("jerry"),
		Type:  PUT,
	}
	res1, n1 := EncodeLogRecord(rec1)
	t.Log(res1)
	assert.NotNil(t, res1)
	assert.Greater(t, n1, int64(5))

	//if value is empty
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: PUT,
	}
	res2, n2 := EncodeLogRecord(rec2)
	t.Log(res2)
	assert.NotNil(t, res2)
	assert.Greater(t, n2, int64(5))
	// 对 Deleted 情况的测试
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("jerry"),
		Type:  DELETE,
	}
	res3, n3 := EncodeLogRecord(rec3)
	t.Log(res3)
	assert.NotNil(t, res3)
	assert.Greater(t, n3, int64(5))
}
func TestDecodeLogRecordHeader(t *testing.T) {
	//DecodeLogRecordHeader()
	headerBuf1 := []byte{51, 248, 83, 80, 0, 8, 10}
	h1, size1 := DecodeLogRecordHeader(headerBuf1)
	t.Log(h1)
	t.Log(size1)
	assert.Equal(t, int64(7), size1)
	assert.Equal(t, uint32(1347680307), h1.CRC)
	//
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size2 := DecodeLogRecordHeader(headerBuf2)
	assert.NotNil(t, h2)
	assert.Equal(t, int64(7), size2)
	assert.Equal(t, uint32(240712713), h2.CRC)
	assert.Equal(t, PUT, h2.Type)
	assert.Equal(t, uint32(4), h2.KeySize)
	assert.Equal(t, uint32(0), h2.ValueSize)
	//
	headerBuf3 := []byte{92, 180, 246, 203, 1, 8, 10}
	h3, size3 := DecodeLogRecordHeader(headerBuf3)
	assert.NotNil(t, h3)
	assert.Equal(t, int64(7), size3)
	assert.Equal(t, uint32(3421942876), h3.CRC)
	assert.Equal(t, DELETE, h3.Type)
	assert.Equal(t, uint32(4), h3.KeySize)
	assert.Equal(t, uint32(5), h3.ValueSize)
}

func TestGetCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("jerry"),
		Type:  PUT,
	}
	headerBuf1 := []byte{51, 248, 83, 80, 0, 8, 10}
	crc1 := getCRC(rec1, headerBuf1[4:])
	assert.Equal(t, uint32(1347680307), crc1)

	//
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: PUT,
	}
	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getCRC(rec2, headerBuf2[crc32.Size:])
	assert.Equal(t, uint32(240712713), crc2)
	//
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("jerry"),
		Type:  DELETE,
	}
	headerBuf3 := []byte{92, 180, 246, 203, 1, 8, 10}
	crc3 := getCRC(rec3, headerBuf3[crc32.Size:])
	assert.Equal(t, uint32(3421942876), crc3)
}
