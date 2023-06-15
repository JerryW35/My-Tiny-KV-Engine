package index_test

import (
	"KVstore/data"
	"KVstore/index"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Put(t *testing.T) {
	bt := index.NewBTree()
	res := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 111})
	assert.True(t, res)
	res1 := bt.Put([]byte("key1"), &data.LogRecordPos{Fid: 2, Offset: 222})
	assert.True(t, res1)
}

func TestBTree_Get(t *testing.T) {
	bt := index.NewBTree()
	bt.Put([]byte("key1"), &data.LogRecordPos{Fid: 2, Offset: 222})
	res := bt.Get([]byte("key1"))
	assert.Equal(t, uint32(2), res.Fid)
	assert.Equal(t, int64(222), res.Offset)
}
func TestBTree_Delete(t *testing.T) {
	bt := index.NewBTree()
	bt.Put([]byte("key1"), &data.LogRecordPos{Fid: 2, Offset: 222})
	res := bt.Delete([]byte("key1"))
	assert.True(t, res)
	res1 := bt.Get([]byte("key1"))
	assert.Nil(t, res1)
}
func TestBTree_Iterator(t *testing.T) {

	bt1 := index.NewBTree()
	// 1.BTree is nil
	iter1 := bt1.Iterator(false)
	assert.Equal(t, false, iter1.Valid())

	// 2. Btree has valid value
	bt1.Put([]byte("test"), &data.LogRecordPos{Fid: 1, Offset: 10})
	iter2 := bt1.Iterator(false)
	assert.Equal(t, true, iter2.Valid())
	t.Log(iter2.Key())
	t.Log(iter2.Value())
	assert.NotNil(t, iter2.Key())
	assert.NotNil(t, iter2.Value())
	iter2.Next()
	assert.Equal(t, false, iter2.Valid())
	// 3.has a lot values
	bt1.Put([]byte("1"), &data.LogRecordPos{Fid: 1, Offset: 1})
	bt1.Put([]byte("2"), &data.LogRecordPos{Fid: 1, Offset: 2})
	bt1.Put([]byte("3"), &data.LogRecordPos{Fid: 1, Offset: 3})
	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	// 4.test seek
	iter5 := bt1.Iterator(false)
	for iter5.Seek([]byte("5")); iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	// 5.reverse test
	iter6 := bt1.Iterator(true)
	for iter6.Seek([]byte("2")); iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}
}
