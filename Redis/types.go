package Redis

import (
	"KVstore"
	"encoding/binary"
	"errors"
	"time"
)

type redisDataType = byte

const (
	String redisDataType = iota
	Hash
	Set
	List
	ZSet
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")
)

type RedisDataStructure struct {
	db *KVstore.DB
}

func newRedisDataStructure(configs KVstore.Configs) (*RedisDataStructure, error) {
	db, err := KVstore.Open(configs)
	if err != nil {
		return nil, err
	}
	return &RedisDataStructure{db: db}, nil
}

// =============================String================================
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, values []byte) error {
	if values == nil {
		return nil
	}
	// encode value:type+expire+payloads
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = String
	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}
	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(values))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], values)

	//write data into database
	return rds.db.Put(key, encValue)
}
func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}
	// decode the value to get type, expire time, payload
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}
	var index = 1
	expire, n := binary.Varint(encValue[index:])
	index += n
	//check if expired
	if expire > 0 && time.Now().UnixNano() >= expire {
		return nil, nil
	}
	return encValue[index:], nil
}

// =============================Hash================================
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()
	// return true if field not exist else false
	//find whether exists
	var exist = true
	if _, err := rds.db.Get(encKey); err == KVstore.ErrorKeyNotFound {
		exist = false
	}
	wb := rds.db.NewWriteBatch(KVstore.DefaultWriteBatchConfigs)
	//if not exist, update metadata
	if !exist {
		meta.size++
		_ = wb.Put(key, meta.encodeMetaData())
	}
	err = wb.Put(encKey, value)
	if err = wb.Commit(); err != nil {
		return false, err
	}
	return !exist, nil
}
func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return nil, err
	}
	if meta.size == 0 {
		return nil, nil
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	return rds.db.Get(hk.encode())

}
func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	meta, err := rds.findMetaData(key, Hash)
	if err != nil {
		return false, err
	}
	if meta.size == 0 {
		return false, nil
	}
	hk := &hashInternalKey{
		key:     key,
		version: meta.version,
		field:   field,
	}
	encKey := hk.encode()
	//check if exists
	var exist = true
	if _, err = rds.db.Get(encKey); err == KVstore.ErrorKeyNotFound {
		exist = false
	}
	if exist {
		wb := rds.db.NewWriteBatch(KVstore.DefaultWriteBatchConfigs)
		meta.size--
		_ = wb.Put(key, meta.encodeMetaData())
		_ = wb.Delete(encKey)
		if err = wb.Commit(); err != nil {
			return false, err
		}
	}
	return exist, nil
}
func (rds *RedisDataStructure) findMetaData(key []byte, dataType redisDataType) (*metadata, error) {
	metaBuf, err := rds.db.Get(key)
	if err != nil && err != KVstore.ErrorKeyNotFound {
		return nil, err
	}
	// do some check
	var meta *metadata
	var exist = true
	if err == KVstore.ErrorKeyNotFound {
		exist = false
	} else {
		// exist, check the type
		meta = decodeMetaData(metaBuf)
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		//check expire
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	// init
	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
