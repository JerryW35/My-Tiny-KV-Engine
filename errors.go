package KVstore

import "errors"

var (
	ErrorKeyEmpty     = errors.New("key is empty")
	ErrorUpdateIndex  = errors.New("cannot update index")
	ErrorInvalidKey   = errors.New("no such key")
	ErrorKeyNotFound  = errors.New("key not found")
	ErrorFileNotFound = errors.New("data file not found")
)
