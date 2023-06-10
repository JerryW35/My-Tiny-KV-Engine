package KVstore

import (
	"KVstore/data"
	"KVstore/index"
	"sync"
)

// some APIs for the user

type DB struct {
	config     configs
	mutex      *sync.RWMutex
	activeFile *data.File
	olderFiles map[uint32]*data.File
	index      index.Indexer
}

func (db *DB) Put(key []byte, value []byte) error {
	//check if the key is empty
	if len(key) == 0 {
		return ErrorKeyEmpty
	}
	//construct the log record
	logRecord := data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.PUT,
	}
	pos, err := db.appendLogRecord(&logRecord)
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
	//1.check if is in the active file
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
	logRecord, err := dataFile.Read(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	// check the type of logRecord
	if logRecord.Type == data.DELETE {
		return nil, ErrorKeyNotFound
	}
	return logRecord.Value, nil
}
func (db *DB) appendLogRecord(record *data.LogRecord) (*data.LogRecordPos, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

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

//check the
