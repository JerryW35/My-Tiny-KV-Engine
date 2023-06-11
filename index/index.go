package index

import (
	"KVstore/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	// Put the key and the location of the data
	Put(key []byte, pos *data.LogRecordPos) bool
	// Get the location of the data by the key
	Get(key []byte) *data.LogRecordPos
	// Delete the item by the key
	Delete(key []byte) bool
}
type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
)

// init Indexer by IndexType
func NewIndexr(typ IndexType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return nil
	default:
		panic("unsupported idnex type")
	}
}

// Item is our node type for the btree
type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (a *Item) Less(b btree.Item) bool {
	return bytes.Compare(a.key, b.(*Item).key) == -1
}
