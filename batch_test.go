package KVstore

import (
	"KVstore/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestDB_WriteBatch1(t *testing.T) {
	opts := DefaultConfigs
	dir, _ := os.MkdirTemp("./", "bitcask-go-batch-1")
	opts.DirPath = dir + "/"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	// write without commit
	wb := db.NewWriteBatch(DefaultWriteBatchConfigs)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(2))
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrorKeyNotFound, err)

	// commit
	err = wb.Commit()
	assert.Nil(t, err)

	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	// delete a key
	wb2 := db.NewWriteBatch(DefaultWriteBatchConfigs)
	err = wb2.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)
	err = wb2.Commit()
	assert.Nil(t, err)

	_, err = db.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrorKeyNotFound, err)
}
func TestDB_WriteBatch2(t *testing.T) {
	opts := DefaultConfigs
	dir, _ := os.MkdirTemp("./", "bitcask-go-batch-2")

	opts.DirPath = dir + "/"
	db, err := Open(opts)

	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)

	wb := db.NewWriteBatch(DefaultWriteBatchConfigs)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)

	err = wb.Put(utils.GetTestKey(11), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Commit()
	assert.Nil(t, err)

	// restart db
	err = db.Close()
	assert.Nil(t, err)
	destroyDB(db)

	db2, err := Open(opts)
	defer destroyDB(db2)
	assert.Nil(t, err)

	_, err = db2.Get(utils.GetTestKey(1))
	assert.Equal(t, ErrorKeyNotFound, err)

	// check seqNo
	assert.Equal(t, uint64(2), db.seqNo)

}
func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultConfigs
	dir, _ := os.MkdirTemp("./", "bitcask-go-batch-3")
	//dir := "./bitcask-go-batch-3"
	opts.DirPath = dir + "/"
	db, err := Open(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	t.Log(len(keys))

	wbConfigs := DefaultWriteBatchConfigs
	wbConfigs.MaxBatchNum = 10000000
	wb := db.NewWriteBatch(wbConfigs)
	for i := 0; i < 500000; i++ {
		err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(t, err)
	}
	err = wb.Commit()
	assert.Nil(t, err)
}
