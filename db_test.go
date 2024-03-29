package KVstore

import (
	"KVstore/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestDB_ListKeys(t *testing.T) {
	var ok bool
	t.Log(ok)
}

// delete test files
func destroyDB(db *DB) {
	if db != nil {
		if db.activeFile != nil {
			_ = db.Close()
		}
		err := os.RemoveAll(db.config.DirPath)
		if err != nil {
			panic(err)
		}
	}
}

func TestOpen(t *testing.T) {
	configs := DefaultConfigs
	dir, _ := os.MkdirTemp("./", "tests")
	t.Log(dir)
	configs.DirPath = dir
	db, err := Open(configs)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)
}

func TestDB_Put(t *testing.T) {
	configs := DefaultConfigs
	dir, _ := os.MkdirTemp("./", "tests")
	configs.DirPath = dir
	configs.DataFileSize = 64 * 1024 * 1024
	db, err := Open(configs)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1. Put one log
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.repeat Put key with same key and value
	err = db.Put(utils.GetTestKey(1), utils.RandomValue(24))
	assert.Nil(t, err)
	val2, err := db.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	// 3.key is nil
	err = db.Put(nil, utils.RandomValue(24))
	assert.Equal(t, ErrorKeyEmpty, err)

	// 4.value is nil
	err = db.Put(utils.GetTestKey(22), nil)
	assert.Nil(t, err)
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Equal(t, 0, len(val3))
	assert.Nil(t, err)

	// 5.write until full
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 0, len(db.olderFiles))

	// 6.restart and put&read again
	db.Close() //
	//err = db.activeFile.Close()
	assert.Nil(t, err)

	//restart database
	db2, err := Open(configs)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	val4 := utils.RandomValue(128)
	err = db2.Put(utils.GetTestKey(55), val4)
	assert.Nil(t, err)
	val5, err := db2.Get(utils.GetTestKey(55))
	t.Log(string(val4))
	t.Log(string(val5))
	assert.Nil(t, err)
	assert.Equal(t, val4, val5)
}

func TestDB_Get(t *testing.T) {
	configs := DefaultConfigs
	dir, _ := os.MkdirTemp("", "tests")
	configs.DirPath = dir + "/"
	configs.DataFileSize = 64 * 1024 * 1024
	db, err := Open(configs)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.read one log
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(24))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	// 2.read not existed log
	val2, err := db.Get([]byte("some key unknown"))
	assert.Nil(t, val2)
	assert.Equal(t, ErrorKeyNotFound, err)

	// 3.put repeated value then read
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(24))
	val3, err := db.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val3)

	// 4. delete then read
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(24))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(33))
	assert.Nil(t, err)
	val4, err := db.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val4))
	assert.Equal(t, ErrorKeyNotFound, err)

	// 5.transfer to older file and read from it
	for i := 100; i < 1000000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	assert.Equal(t, 2, len(db.olderFiles))
	val5, err := db.Get(utils.GetTestKey(101))
	assert.Nil(t, err)
	assert.NotNil(t, val5)

	// 6. restart and get all logs
	db.Close()
	//err = db.activeFile.Close()
	assert.Nil(t, err)

	db2, err := Open(configs)
	val6, err := db2.Get(utils.GetTestKey(11))
	assert.Nil(t, err)
	assert.NotNil(t, val6)
	assert.Equal(t, val1, val6)

	val7, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.NotNil(t, val7)
	assert.Equal(t, val3, val7)

	val8, err := db2.Get(utils.GetTestKey(33))
	assert.Equal(t, 0, len(val8))
	assert.Equal(t, ErrorKeyNotFound, err)
}

func TestDB_Delete(t *testing.T) {
	configs := DefaultConfigs
	dir, _ := os.MkdirTemp("", "tests")
	configs.DirPath = dir + "/"
	configs.DataFileSize = 64 * 1024 * 1024
	db, err := Open(configs)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// 1.delete a key
	err = db.Put(utils.GetTestKey(11), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(11))
	assert.Nil(t, err)
	_, err = db.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrorKeyNotFound, err)

	// 2.delete a not existed key
	err = db.Delete([]byte("unknown key"))
	assert.Nil(t, err)

	// 3.delete nil key
	err = db.Delete(nil)
	assert.Equal(t, ErrorKeyEmpty, err)

	// 4. delete then put
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	err = db.Delete(utils.GetTestKey(22))
	assert.Nil(t, err)

	err = db.Put(utils.GetTestKey(22), utils.RandomValue(128))
	assert.Nil(t, err)
	val1, err := db.Get(utils.GetTestKey(22))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// 5.after restart then check
	db.Close()
	//err = db.activeFile.Close()
	assert.Nil(t, err)

	// restart database
	db2, err := Open(configs)
	_, err = db2.Get(utils.GetTestKey(11))
	assert.Equal(t, ErrorKeyNotFound, err)

	val2, err := db2.Get(utils.GetTestKey(22))
	assert.Nil(t, err)
	assert.Equal(t, val1, val2)
}
func TestDB_Fold(t *testing.T) {
	opts := DefaultConfigs
	dir, _ := os.MkdirTemp("", "bitcask-go-fold")
	opts.DirPath = dir + "/"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(22), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(33), utils.RandomValue(20))
	assert.Nil(t, err)
	err = db.Put(utils.GetTestKey(44), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Fold(func(key []byte, value []byte) bool {
		assert.NotNil(t, key)
		assert.NotNil(t, value)
		return true
	})
	assert.Nil(t, err)
}

func TestDB_Close(t *testing.T) {
	opts := DefaultConfigs
	dir, _ := os.MkdirTemp("", "bitcask-go-close")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)
}

func TestDB_Sync(t *testing.T) {
	opts := DefaultConfigs
	dir, _ := os.MkdirTemp("", "bitcask-go-sync")
	opts.DirPath = dir
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(11), utils.RandomValue(20))
	assert.Nil(t, err)

	err = db.Sync()
	assert.Nil(t, err)
}
func TestDB_FileLock(t *testing.T) {
	opts := DefaultConfigs
	dir, _ := os.MkdirTemp("./", "bitcask-go-filelock")
	opts.DirPath = dir + "/"

	db, err := Open(opts)
	defer destroyDB(db)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	_, err = Open(opts)
	assert.Equal(t, ErrorDataBaseIsInUse, err)

	err = db.Close()
	assert.Nil(t, err)
	//
	db2, err := Open(opts)
	t.Log(err)
	t.Log(db2)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	err = db2.Close()
	assert.Nil(t, err)
}

// 5000000 data 12 files in total
// false : 50s
// true:	7s
func TestDB_OpenMMap(t *testing.T) {
	opts := DefaultConfigs
	opts.DirPath = "/Users/wzr/Desktop/KVstore/tests2195224528/"
	opts.MMapLoad = true

	now := time.Now()
	db, err := Open(opts)
	t.Log("open time ", time.Since(now))

	assert.Nil(t, err)
	assert.NotNil(t, db)
}
func TestDB_Stat(t *testing.T) {
	opts := DefaultConfigs
	dir, _ := os.MkdirTemp("./", "bitcask-go-stat")
	opts.DirPath = dir + "/"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	for i := 100; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}
	for i := 100; i < 1000; i++ {
		err := db.Delete(utils.GetTestKey(i))
		assert.Nil(t, err)
	}
	for i := 2000; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(128))
		assert.Nil(t, err)
	}

	stat := db.Stat()
	t.Log(stat)
	assert.NotNil(t, stat)
}

func TestDB_Backup(t *testing.T) {
	opts := DefaultConfigs
	dir := "/Users/wzr/Downloads/test"
	opts.DirPath = dir
	db, err := Open(opts)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	db.Put(utils.GetTestKey(1), utils.RandomValue(128))
	//
	backupDir := "/Users/wzr/Downloads/backup"
	err = db.Backup(backupDir)
	assert.Nil(t, err)

	opts1 := DefaultConfigs
	opts1.DirPath = backupDir
	db2, err := Open(opts1)
	assert.Nil(t, err)
	assert.NotNil(t, db2)
	destroyDB(db)
	destroyDB(db2)
}
