package KVstore

import (
	"KVstore/index"
	"bytes"
)

// Iterator for user
type Iterator struct {
	indexIter index.IndexrIterator
	db        *DB
	config    IteratorConfigs
}

func (db *DB) NewIterator(config IteratorConfigs) *Iterator {
	indexIter := db.index.Iterator(config.Reverse)
	return &Iterator{
		indexIter: indexIter,
		db:        db,
		config:    config,
	}
}
func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.Filter()
}
func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.Filter()
}
func (it *Iterator) Next() {
	it.indexIter.Next()
	it.Filter()
}
func (it *Iterator) Close() {
	it.indexIter.Close()
}
func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}
func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mutex.RLock()
	defer it.db.mutex.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}
func (it *Iterator) Filter() {
	prefixLens := len(it.config.Prefix)
	if prefixLens == 0 {
		return
	}
	for ; it.indexIter.Valid(); it.indexIter.Next() {
		key := it.indexIter.Key()
		if prefixLens <= len(key) && bytes.Compare(it.config.Prefix, key[:prefixLens]) == 0 {
			break
		}

	}
}
