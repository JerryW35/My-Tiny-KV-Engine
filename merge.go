package KVstore

import (
	"KVstore/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
)

const (
	mergeDirName     = "_merge"
	mergeFinishedKey = "merge_FIN"
)

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mutex.Lock()

	if db.isMerging {
		db.mutex.Unlock()
		return ErrorIsMerging
	}

	db.isMerging = true
	defer func() {
		db.isMerging = false
	}()

	if err := db.activeFile.Sync(); err != nil {
		db.mutex.Unlock()
		return err
	}
	// transfer active file to old file
	db.olderFiles[db.activeFile.FileId] = db.activeFile
	// create a new file
	if err := db.setActivateFile(); err != nil {
		db.mutex.Unlock()
		return err
	}
	//get fileId that not be merged
	nonMergeFileId := db.activeFile.FileId
	// get old files
	var mergeFiles []*data.File
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)
	}
	db.mutex.Unlock()

	// from small to big
	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	mergePath := db.getMergePath()
	// if merge dir exist, remove it
	if _, err := os.Stat(mergePath); err == nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}
	// create merge dir
	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}
	// get new db instance
	mergeDB, err := Open(Configs{
		DirPath:      mergePath,
		IndexerType:  db.config.IndexerType,
		SyncWrites:   false,
		DataFileSize: db.config.DataFileSize,
	})
	if err != nil {
		return err
	}
	// open hint file
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}
	for _, dataFile := range mergeFiles {
		var offset int64 = 0
		for {
			logRecord, size, err := dataFile.Read(offset)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			// get real key
			realKey, _ := parseKeyWithSeqNo(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			//compare logRecordPos from index and logRecordPos from dataFile
			if logRecordPos != nil &&
				logRecordPos.Fid == dataFile.FileId &&
				logRecordPos.Offset == offset {
				// don't need SeqNo again
				logRecord.Key = logRecordKeyWithSeqNo(realKey, NonTxnSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}
				//write logRecordPos to HintFile
				if err := hintFile.WriteHintRecord(realKey, pos); err != nil {
					return err
				}

			}
			offset += size
		}
	}
	err = hintFile.Sync()
	if err != nil {
		return err
	}
	err = mergeDB.Sync()
	if err != nil {
		return err
	}
	mergeFinFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return err
	}
	mergeFinRecord := data.LogRecord{
		Key:   []byte(mergeFinishedKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}
	encRecord, _ := data.EncodeLogRecord(&mergeFinRecord)
	if err := mergeFinFile.Write(encRecord); err != nil {
		return err
	}
	err = mergeFinFile.Sync()
	if err != nil {
		return err
	}
	return nil
}
func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.config.DirPath))
	base := path.Base(db.config.DirPath)
	return filepath.Join(dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return nil
	}
	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dirEntries, err := os.ReadDir(mergePath)
	if err != nil {
		return err
	}
	// find merge FIN file
	var mergeFinished bool
	var mergeFileNames []string
	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishedFileName {
			mergeFinished = true
		}
		if entry.Name() == data.SeqNoFileName {
			continue
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	if !mergeFinished {
		return nil
	}

	//get non Merged file ID
	nonMergeFileId, err := db.getNonMergeFileID(mergePath)
	if err != nil {
		return err
	}
	// delete old data files
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(db.config.DirPath, fileId)
		if _, err := os.Stat(fileName); err == nil {
			if err := os.Remove(fileName); err != nil {
				return err
			}
		}
	}
	// move new data file to dir
	for _, fileName := range mergeFileNames {
		srcPath := filepath.Join(mergePath, fileName)
		destPath := filepath.Join(db.config.DirPath, fileName)
		if err := os.Rename(srcPath, destPath); err != nil {
			return err
		}
	}
	return nil

}

func (db *DB) getNonMergeFileID(mergePath string) (uint32, error) {
	mergeFinFile, err := data.OpenMergeFinishedFile(mergePath)
	if err != nil {
		return 0, err
	}
	record, _, err := mergeFinFile.Read(0)
	if err != nil {
		return 0, err
	}
	nonMergeFileId, err := strconv.Atoi(string(record.Value))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil

}
func (db *DB) loadIndexFromHint() error {
	fileName := filepath.Join(db.config.DirPath, data.HintFileName)
	if _, err := os.Stat(fileName); os.IsNotExist(err) {
		return nil
	}
	// open hint file
	hintFile, err := data.OpenHintFile(fileName)
	if err != nil {
		return err
	}

	// load index according to hintFile
	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil

}
