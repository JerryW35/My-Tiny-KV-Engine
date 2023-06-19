package index

import (
	"KVstore/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	// Put the key and the location of the data
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	// Get the location of the data by the key
	Get(key []byte) *data.LogRecordPos
	// Delete the item by the key
	Delete(key []byte) (*data.LogRecordPos, bool)

	Iterator(reverse bool) IndexrIterator
	Size() int
	Close() error
}

// generic IndexerIterator
type IndexrIterator interface {
	Rewind() //Back to the beginning
	// Seek Find the first target key that is greater (or less) than or equal
	// to the incoming key, then iterate from this key
	Seek(key []byte)

	Next()                     // the next key
	Valid() bool               // if the key and the position is valid, for exit the iteration
	Key() []byte               //current key
	Value() *data.LogRecordPos ///current value position
	Close()
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART
	BPTree
)

// init Indexer by IndexType
func NewIndexr(typ IndexType, path string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		//return NewART()
		return nil
	case BPTree:
		return nil
		//return NewBPlusTree(path, sync)

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
