package index

import (
	"KVstore/data"
	"bytes"
	"github.com/google/btree"
	"sort"
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

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := Item{key: key, pos: pos}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.ReplaceOrInsert(&it)
	if oldItem == nil {
		return nil
	}
	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := Item{key: key}
	lookUpvalue := bt.tree.Get(&it)
	if lookUpvalue == nil {
		return nil
	}
	return lookUpvalue.(*Item).pos

}
func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()
	oldItem := bt.tree.Delete(&it)
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}
func (bt *BTree) Size() int {
	return bt.tree.Len()
}
func (bt *BTree) Close() error {
	return nil
}

/*
b tree iterator
*/
type BTreeIterator struct {
	currentIndex int
	reverse      bool    // whether iterate reversely
	values       []*Item // is same as tree node
}

func (bt BTree) Iterator(reverse bool) IndexrIterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}
func newBTreeIterator(tree *btree.BTree, reverse bool) *BTreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	//put all logs into memory(values)
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}
	return &BTreeIterator{
		reverse:      reverse,
		currentIndex: 0,
		values:       values,
	}
}

func (it *BTreeIterator) Rewind() {
	it.currentIndex = 0
}

func (it *BTreeIterator) Seek(key []byte) {
	// use binary search to speed up
	if it.reverse {
		it.currentIndex = sort.Search(len(it.values), func(i int) bool {
			return bytes.Compare(it.values[i].key, key) <= 0
		})
	} else {
		it.currentIndex = sort.Search(len(it.values), func(i int) bool {
			return bytes.Compare(it.values[i].key, key) >= 0
		})
	}
}

func (it *BTreeIterator) Next() {
	it.currentIndex++
}

func (it *BTreeIterator) Valid() bool {
	return it.currentIndex < len(it.values)
}

func (it *BTreeIterator) Key() []byte {
	return it.values[it.currentIndex].key
}

func (it *BTreeIterator) Value() *data.LogRecordPos {
	return it.values[it.currentIndex].pos
}

func (it *BTreeIterator) Close() {
	it.values = nil
}
