package KVstore

import (
	"KVstore/data"
	"KVstore/index"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

type WriteBatch struct {
	mutex         *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
	configs       WriteBatchConfigs
}

const NonTxnSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

func (db *DB) NewWriteBatch(config WriteBatchConfigs) *WriteBatch {
	if db.config.IndexerType == index.BPTree && !db.seqNoFileExist && !db.isInitial {
		panic("cannot use write batch,seq no file not exists")
	}
	return &WriteBatch{
		configs:       config,
		mutex:         new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// Put write logs
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrorKeyEmpty
	}
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	// temporarily store logs in memory
	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.PUT,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Delete log
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrorKeyEmpty
	}
	wb.mutex.Lock()
	defer wb.mutex.Unlock()
	//check if in the pendingWrites
	if wb.db.index.Get(key) == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}
	// temporarily store logs in memory
	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.DELETE,
	}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// Commit, write all pendingWrites to the disk
func (wb *WriteBatch) Commit() error {
	wb.mutex.Lock()
	defer wb.mutex.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}
	if uint(len(wb.pendingWrites)) > wb.configs.MaxBatchNum {
		return ErrorExceedMaxBatchNum
	}
	wb.db.mutex.Lock()
	defer wb.db.mutex.Unlock()
	// get new SeqNo
	SeqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	// write logs into datafile
	tempPos := make(map[string]*data.LogRecordPos)
	for _, record := range wb.pendingWrites {
		logRecordPos, err := wb.db.appendLogRecord(&data.LogRecord{
			Key:   logRecordKeyWithSeqNo(record.Key, SeqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		tempPos[string(record.Key)] = logRecordPos
	}
	//add finish flag for transaction
	finishedRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(txnFinKey, SeqNo),
		Type: data.COMMIT,
	}
	if _, err := wb.db.appendLogRecord(finishedRecord); err != nil {
		return err
	}

	//check if need to do persistence
	if wb.configs.SyncWrites {
		err := wb.db.activeFile.Sync()
		if err != nil {
			return err
		}
	}

	//update indexer
	for _, record := range wb.pendingWrites {
		pos := tempPos[string(record.Key)]
		var oldPos *data.LogRecordPos
		if record.Type == data.PUT {
			oldPos = wb.db.index.Put(record.Key, pos)

		} else if record.Type == data.DELETE {
			oldPos, _ = wb.db.index.Delete(record.Key)
		}
		if oldPos != nil {
			wb.db.reclaimSize += int64(oldPos.Size)
		}
	}
	// clean
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

func logRecordKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(seq[:], seqNo)
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}
func parseKeyWithSeqNo(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
