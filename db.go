package KVstore

import (
	"KVstore/data"
	"KVstore/index"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// some APIs for the user

type DB struct {
	config     *Configs
	mutex      *sync.RWMutex
	activeFile *data.File
	olderFiles map[uint32]*data.File
	index      index.Indexer
	fileIds    []int // only used for loading index
	seqNo      uint64
	isMerging  bool
}

/*
APIs for user
*/
func (db *DB) Put(key []byte, value []byte) error {
	//check if the key is empty
	if len(key) == 0 {
		return ErrorKeyEmpty
	}
	//construct the log record
	logRecord := data.LogRecord{
		Key:   logRecordKeyWithSeqNo(key, NonTxnSeqNo),
		Value: value,
		Type:  data.PUT,
	}
	pos, err := db.appendLogRecordWithLock(&logRecord)
	if err != nil {
		return err
	}
	//update index
	ok := db.index.Put(key, pos)
	if !ok {
		return ErrorUpdateIndex
	}
	return nil
}
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	// check if the key valid or exists
	if len(key) == 0 {
		return nil, ErrorInvalidKey
	}
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, ErrorKeyNotFound
	}
	//get the value from the file
	return db.getValueByPosition(logRecordPos)
}
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrorKeyEmpty
	}
	//check if key exists in the indexer
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	//add a tombstone record
	logRecord := data.LogRecord{Key: logRecordKeyWithSeqNo(key, NonTxnSeqNo), Type: data.DELETE}
	_, err := db.appendLogRecordWithLock(&logRecord)
	if err != nil {
		return err
	}
	ok := db.index.Delete(key)
	if !ok {
		return ErrorUpdateIndex
	}
	return nil
}
func (db *DB) ListKeys() [][]byte {
	iter := db.index.Iterator(false)
	keys := make([][]byte, db.index.Size())
	var idx int
	for iter.Rewind(); iter.Valid(); iter.Next() {
		keys[idx] = iter.Key()
		idx++
	}
	return keys
}

// Fold get all keys and values, satisfy UDF, when get false return
func (db *DB) Fold(fn func(key []byte, value []byte) bool) error {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	iter := db.index.Iterator(false)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		val, err := db.getValueByPosition(iter.Value())
		if err != nil {
			return err
		}
		if !fn(iter.Key(), val) {
			break
		}
	}
	return nil
}

func Open(configs Configs) (*DB, error) {
	// firstly check the config
	err := checkConfigs(configs)
	if err != nil {
		return nil, err
	}
	//check the dir, if not exist then create a new one
	if _, err := os.Stat(configs.DirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(configs.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}
	//init DB structure
	db := &DB{
		config:     &configs,
		mutex:      new(sync.RWMutex),
		olderFiles: make(map[uint32]*data.File),
		index:      index.NewIndexr(configs.IndexerType),
	}
	// load merge files
	if err := db.loadMergeFiles(); err != nil {
		return nil, err
	}
	// load files
	if err := db.loadFiles(); err != nil {
		return nil, err
	}
	if err := db.loadIndexFromHint(); err != nil {
		return nil, err
	}
	// load indexer
	if err := db.loadIndexer(); err != nil {
		return nil, err
	}
	return db, nil
}
func (db *DB) Close() error {
	if db.activeFile == nil {
		return nil
	}
	db.mutex.Lock()
	defer db.mutex.Unlock()

	// close active file
	if err := db.activeFile.Close(); err != nil {
		return err
	}
	// close old files
	for _, file := range db.olderFiles {
		if err := file.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Sync do persistence
func (db *DB) Sync() error {
	if db.activeFile == nil {
		return nil
	}
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.activeFile.Sync()
}

/*
some useful methods
*/
func (db *DB) appendLogRecordWithLock(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	return db.appendLogRecord(record)
}
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	// check if exist active file
	// if not, create a new file
	if db.activeFile == nil {
		if err := db.setActivateFile(); err != nil {
			return nil, err
		}
	}
	//write data
	encRecord, lens := data.EncodeLogRecord(record)

	//check if threshold value exceeded
	if db.activeFile.WriteOffset+lens > db.config.DataFileSize {
		//firstly persist the Datafile
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
		// activeFile -> OlderFile
		db.olderFiles[db.activeFile.FileId] = db.activeFile
		// open a new Datafile
		if err := db.setActivateFile(); err != nil {
			return nil, err
		}
	}
	// write data into file
	writeOff := db.activeFile.WriteOffset
	if err := db.activeFile.Write(encRecord); err != nil {
		return nil, err
	}
	// check users want to persist
	if db.config.SyncWrites {
		if err := db.activeFile.Sync(); err != nil {
			return nil, err
		}
	}
	pos := &data.LogRecordPos{
		Fid:    db.activeFile.FileId,
		Offset: writeOff,
	}
	return pos, nil
}

// set the active file
// need a mutex before reaching this func
func (db *DB) setActivateFile() error {

	var initialFileId uint32 = 0
	if db.activeFile != nil {
		initialFileId = db.activeFile.FileId + 1
	}
	// open a new file
	dataFile, err := data.OpenFile(db.config.DirPath, initialFileId)
	if err != nil {
		return err
	}
	db.activeFile = dataFile
	return nil
}

// load files from local disk
func (db *DB) loadFiles() error {
	dirs, err := os.ReadDir(db.config.DirPath)
	if err != nil {
		return ErrorLoadFiles
	}
	var fileIds []int
	//find files with suffix .data
	for _, dir := range dirs {
		if strings.HasSuffix(dir.Name(), data.FileSuffix) {
			prefix := strings.Split(dir.Name(), ".")
			fileId, err := strconv.Atoi(prefix[0])
			if err != nil {
				return ErrorParse
			}
			fileIds = append(fileIds, fileId)
		}
	}
	sort.Ints(fileIds)
	db.fileIds = fileIds
	//loop all files and open the files
	for i, id := range fileIds {
		dataFile, err := data.OpenFile(db.config.DirPath, uint32(id))
		if err != nil {
			return err
		}
		if i == len(fileIds)-1 {
			db.activeFile = dataFile
		} else {
			db.olderFiles[uint32(id)] = dataFile
		}
	}
	return nil
}

// from files to load indexer
func (db *DB) loadIndexer() error {
	if len(db.fileIds) == 0 {
		return nil
	}
	// check if merge happened
	hasMerge, nonMergeFileId := false, uint32(0)
	mergeFinFileName := filepath.Join(db.config.DirPath, data.MergeFinishedFileName)
	if _, err := os.Stat(mergeFinFileName); err == nil {
		fid, err := db.getNonMergeFileID(db.config.DirPath)
		if err != nil {
			return err
		}
		hasMerge = true
		nonMergeFileId = fid
	}

	updateIndex := func(key []byte, typ data.RecordType, pos *data.LogRecordPos) {
		var ok bool
		if typ == data.DELETE {
			ok = db.index.Delete(key)
		} else {
			ok = db.index.Put(key, pos)
		}
		if !ok {
			panic("failed to update index at startup")
		}
	}
	// txn logs
	txnRecords := make(map[uint64][]*data.TxnRecord)
	var curSeqNo = NonTxnSeqNo

	for i, id := range db.fileIds {
		var fileId = uint32(id)
		// check if the file is already loaded from hintFile
		if hasMerge && fileId < nonMergeFileId {
			continue
		}
		var file *data.File
		//load the file
		if fileId == db.activeFile.FileId {
			file = db.activeFile
		} else {
			file = db.olderFiles[fileId]
		}
		var offset int64 = 0
		for {
			logRecord, lens, err := file.Read(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			//create indexer in memory
			logRecordPos := data.LogRecordPos{
				Fid:    file.FileId,
				Offset: offset,
			}
			//update indexer
			//get key and SeqNo
			realKey, SeqNo := parseKeyWithSeqNo(logRecord.Key)
			if SeqNo == NonTxnSeqNo {
				updateIndex(realKey, logRecord.Type, &logRecordPos)
			} else {
				// Txn commit valid
				if logRecord.Type == data.COMMIT {
					for _, txnRecord := range txnRecords[SeqNo] {
						updateIndex(txnRecord.Record.Key, txnRecord.Record.Type, txnRecord.Pos)
					}
				} else {
					txnRecords[SeqNo] = append(txnRecords[SeqNo], &data.TxnRecord{
						logRecord, &logRecordPos,
					})
				}
			}
			if SeqNo > curSeqNo {
				curSeqNo = SeqNo
			}
			//keyWithSeqNo := logRecord.Key
			offset += lens
		}
		//if is the active file,update the WriteOffset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}
	db.seqNo = curSeqNo
	return nil
}
func checkConfigs(config Configs) error {
	if config.DirPath == "" {
		return ConfigErrorDBDirEmpty
	}
	if config.DataFileSize <= 0 {
		return ConfigErrorSize
	}
	return nil
}

func (db *DB) getValueByPosition(logRecordPos *data.LogRecordPos) ([]byte, error) {
	var dataFile *data.File
	if logRecordPos.Fid == db.activeFile.FileId {
		dataFile = db.activeFile
	} else {
		dataFile = db.olderFiles[logRecordPos.Fid]
	}
	if dataFile == nil {
		return nil, ErrorFileNotFound
	}

	//read the file by offset
	logRecord, _, err := dataFile.Read(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	// check the type of logRecord
	if logRecord.Type == data.DELETE {
		return nil, ErrorKeyNotFound
	}
	return logRecord.Value, nil

}
