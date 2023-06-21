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
