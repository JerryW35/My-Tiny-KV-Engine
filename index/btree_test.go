package index_test

import (
	"KVstore/data"
	"KVstore/index"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := index.NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{FId: 1, Offset: 111})
	assert.True(t, res)
	res1 := bt.Put([]byte("key1"), &data.LogRecordPos{FId: 2, Offset: 222})
	assert.True(t, res1)
}

func TestBTree_Get(t *testing.T) {
	bt := index.NewBTree()
	bt.Put([]byte("key1"), &data.LogRecordPos{FId: 2, Offset: 222})
	res := bt.Get([]byte("key1"))
	assert.Equal(t, uint32(2), res.FId)
	assert.Equal(t, int64(222), res.Offset)
}
func TestBTree_Delete(t *testing.T) {
	bt := index.NewBTree()
	bt.Put([]byte("key1"), &data.LogRecordPos{FId: 2, Offset: 222})
	res := bt.Delete([]byte("key1"))
	assert.True(t, res)
	res1 := bt.Get([]byte("key1"))
	assert.Nil(t, res1)
}
