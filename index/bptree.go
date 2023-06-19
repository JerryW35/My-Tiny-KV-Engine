package index

import (
	"KVstore/data"
	"go.etcd.io/bbolt"
	"path/filepath"
)

const (
	bptreeIndexFileName = "bptree_index"
)

var indexBucketName = []byte("bitcask-index")

type BPlusTree struct {
	tree *bbolt.DB
}

func NewBPlusTree(path string, syncWrites bool) *BPlusTree {
	config := bbolt.DefaultOptions
	config.NoSync = !syncWrites
	bptree, err := bbolt.Open(filepath.Join(path+bptreeIndexFileName), 0644, config)
	if err != nil {
		panic("failed to open B+ tree")
	}
	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in B+ Tree")
	}

	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldPos []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldPos = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to put value")
	}
	if len(oldPos) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldPos)
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		value := bucket.Get(key)
		if len(value) != 0 {
			pos = data.DecodeLogRecordPos(value)
		}
		return nil
	}); err != nil {
		panic("failed to get value")
	}
	return pos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldPos []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if oldPos = bucket.Get(key); len(oldPos) != 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to delete value")
	}
	if len(oldPos) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldPos), true
}

func (bpt *BPlusTree) Iterator(reverse bool) IndexrIterator {
	return newBpTreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Size() int {
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size")
	}
	return size
}
func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

/*
Iterator methods
*/

type bptreeIterator struct {
	tx       *bbolt.Tx
	cursor   *bbolt.Cursor
	reverse  bool
	curKey   []byte
	curValue []byte
}

func newBpTreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	return &bptreeIterator{
		tx:      tx,
		cursor:  tx.Cursor(),
		reverse: reverse,
	}
}

func (bpi *bptreeIterator) Rewind() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Last()
	}
	bpi.curKey, bpi.curValue = bpi.cursor.First()
}

func (bpi *bptreeIterator) Seek(key []byte) {
	bpi.curKey, bpi.curValue = bpi.cursor.Seek(key)
}

func (bpi *bptreeIterator) Next() {
	if bpi.reverse {
		bpi.curKey, bpi.curValue = bpi.cursor.Prev()
	} else {
		bpi.curKey, bpi.curValue = bpi.cursor.Next()
	}
}

func (bpi *bptreeIterator) Valid() bool {
	return len(bpi.curKey) == 0
}

func (bpi *bptreeIterator) Key() []byte {
	return bpi.curKey
}

func (bpi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpi.curValue)
}

func (bpi *bptreeIterator) Close() {
	_ = bpi.tx.Rollback()
}
