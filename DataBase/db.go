package DataBase

import (
	"KVstore"
	"KVstore/data"
	"KVstore/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// some APIs for the user

type DB struct {
	config     *KVstore.Configs
	mutex      *sync.RWMutex
	activeFile *data.File
	olderFiles map[uint32]*data.File
	index      index.Indexer
	fileIds    []int // only used for loading index
}

func (db *DB) Put(key []byte, value []byte) error {
	//check if the key is empty
	if len(key) == 0 {
		return KVstore.ErrorKeyEmpty
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
		return KVstore.ErrorUpdateIndex
	}
	return nil
}
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mutex.RLock()
	defer db.mutex.RUnlock()
	// check if the key valid or exists
	if len(key) == 0 {
		return nil, KVstore.ErrorInvalidKey
	}
	logRecordPos := db.index.Get(key)
	if logRecordPos == nil {
		return nil, KVstore.ErrorKeyNotFound
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
		return nil, KVstore.ErrorFileNotFound
	}

	//read the file by offset
	logRecord, _, err := dataFile.Read(logRecordPos.Offset)
	if err != nil {
		return nil, err
	}
	// check the type of logRecord
	if logRecord.Type == data.DELETE {
		return nil, KVstore.ErrorKeyNotFound
	}
	return logRecord.Value, nil
}
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return KVstore.ErrorKeyEmpty
	}
	//check if key exists in the indexer
	if pos := db.index.Get(key); pos == nil {
		return nil
	}
	//add a tombstone record
	logRecord := data.LogRecord{Key: key, Type: data.DELETE}
	_, err := db.appendLogRecord(&logRecord)
	if err != nil {
		return err
	}
	ok := db.index.Delete(key)
	if !ok {
		return KVstore.ErrorUpdateIndex
	}
	return nil
}

func Open(configs KVstore.Configs) (*DB, error) {
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
	// load files
	if err := db.loadFiles(); err != nil {
		return nil, err
	}
	// load indexer
	if err := db.loadIndexer(); err != nil {
		return nil, err
	}
	return db, nil
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

// load files from local disk
func (db *DB) loadFiles() error {
	dirs, err := os.ReadDir(db.config.DirPath)
	if err != nil {
		return KVstore.ErrorLoadFiles
	}
	var fileIds []int
	//find files with suffix .data
	for _, dir := range dirs {
		if strings.HasSuffix(dir.Name(), data.FileSuffix) {
			prefix := strings.Split(dir.Name(), ".")
			fileId, err := strconv.Atoi(prefix[0])
			if err != nil {
				return KVstore.ErrorParse
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
	for i, id := range db.fileIds {
		var fileId = uint32(id)
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
			var ok bool
			if logRecord.Type == data.DELETE {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, &logRecordPos)
			}
			if !ok {
				return KVstore.ErrorUpdateIndex
			}
			offset += lens
		}
		//if is the active file,update the WriteOffset
		if i == len(db.fileIds)-1 {
			db.activeFile.WriteOffset = offset
		}
	}
	return nil
}
func checkConfigs(config KVstore.Configs) error {
	if config.DirPath == "" {
		return KVstore.ConfigErrorDBDirEmpty
	}
	if config.DataFileSize <= 0 {
		return KVstore.ConfigErrorSize
	}
	return nil
}
