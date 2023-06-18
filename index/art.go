package index

import (
	"KVstore/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	art.tree.Insert(key, pos)
	return true
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, deleted := art.tree.Delete(key)
	return deleted
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) IndexrIterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.tree.Size()
}
func (art *AdaptiveRadixTree) Close() error {
	return nil
}

/*
	ART Iterator
*/

/*
b tree iterator
*/
type ARTIterator struct {
	currentIndex int
	reverse      bool    // whether iterate reversely
	values       []*Item // is same as tree node
}

func (art *AdaptiveRadixTree) ArtIterator(reverse bool) IndexrIterator {
	if art.tree == nil {
		return nil
	}
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newARTIterator(art.tree, reverse)
}
func newARTIterator(tree goart.Tree, reverse bool) *ARTIterator {
	var idx int
	values := make([]*Item, tree.Size())
	if reverse {
		idx = tree.Size() - 1
	}
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues)
	return &ARTIterator{
		currentIndex: 0,
		reverse:      reverse,
		values:       values,
	}
}

func (artIter *ARTIterator) Rewind() {
	artIter.currentIndex = 0
}

func (artIter *ARTIterator) Seek(key []byte) {
	// use binary search to speed up
	if artIter.reverse {
		artIter.currentIndex = sort.Search(len(artIter.values), func(i int) bool {
			return bytes.Compare(artIter.values[i].key, key) <= 0
		})
	} else {
		artIter.currentIndex = sort.Search(len(artIter.values), func(i int) bool {
			return bytes.Compare(artIter.values[i].key, key) >= 0
		})
	}
}

func (artIter *ARTIterator) Next() {
	artIter.currentIndex++
}

func (artIter *ARTIterator) Valid() bool {
	return artIter.currentIndex < len(artIter.values)
}

func (artIter *ARTIterator) Key() []byte {
	return artIter.values[artIter.currentIndex].key
}

func (artIter *ARTIterator) Value() *data.LogRecordPos {
	return artIter.values[artIter.currentIndex].pos
}

func (artIter *ARTIterator) Close() {
	artIter.values = nil
}
