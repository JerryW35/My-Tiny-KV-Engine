package Redis

import (
	"KVstore"
	"KVstore/utils"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestRedisDataStructure_Get(t *testing.T) {
	configs := KVstore.DefaultConfigs
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	t.Log(dir)
	configs.DirPath = dir
	rds, err := newRedisDataStructure(configs)
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(100))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.NotNil(t, val1)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	_, err = rds.Get(utils.GetTestKey(33))
	assert.Equal(t, KVstore.ErrorKeyNotFound, err)
}
func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := KVstore.DefaultConfigs
	rds, err := newRedisDataStructure(opts)
	assert.Nil(t, err)

	// del
	err = rds.Del(utils.GetTestKey(11))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(100))
	assert.Nil(t, err)

	// type
	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Nil(t, err)
	assert.Equal(t, String, typ)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	_, err = rds.Get(utils.GetTestKey(1))
	assert.Equal(t, KVstore.ErrorKeyNotFound, err)
}
