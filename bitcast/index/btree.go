package index

import (
	"KVstore/bitcast/data"
	"github.com/google/btree"
	"sync"
)

// Wrapped google's btree library
type BTree struct {
	/*
		In google btree library, it says:
		Write operations are not safe for concurrent mutation by multiple
		goroutines, but Read operations are.
		so we need to use our own lock to protect it.
	*/
	tree *btree.BTree
	lock *sync.RWMutex
}

// NewBTree init and return a new BTree
func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := Item{key: key, pos: pos}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	bt.tree.ReplaceOrInsert(&it)
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := Item{key: key}
	lookUpvalue := bt.tree.Get(&it)
	if lookUpvalue == nil {
		return nil
	}
	return lookUpvalue.(*Item).pos

}
func (bt *BTree) Delete(key []byte) bool {
	it := Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(&it)
	if oldItem == nil {
		return false
	}
	return true
}
