package KVstore

import "errors"

var (
	ErrorKeyEmpty             = errors.New("key is empty")
	ErrorUpdateIndex          = errors.New("cannot update index")
	ErrorInvalidKey           = errors.New("no such key")
	ErrorKeyNotFound          = errors.New("key not found")
	ErrorFileNotFound         = errors.New("data file not found")
	ConfigErrorDBDirEmpty     = errors.New("database dir path is empty")
	ConfigErrorSize           = errors.New("invalid data file size")
	ErrorLoadFiles            = errors.New("cannot load files")
	ErrorParse                = errors.New("the file name may be corrupted ")
	ErrorExceedMaxBatchNum    = errors.New("exceed max batch num")
	ErrorIsMerging            = errors.New("the db is merging")
	ErrorDataBaseIsInUse      = errors.New("the db is in use")
	ConfigErrorMergeRatio     = errors.New("invalid merge ratio")
	ErrorMergeRationUnReached = errors.New("merge ratio is not reached")
	ErrorNoEnoughSpace        = errors.New("no enough space")
)
