package Redis

import (
	"KVstore/utils"
	"encoding/binary"
	"math"
)

const (
	maxMetaDataSize        = 1 + binary.MaxVarintLen64*2 + binary.MaxVarintLen32
	maxMetaDataSizeForList = maxMetaDataSize + binary.MaxVarintLen64*2
	initialListMark        = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte
	expire   int64
	version  int64
	size     uint32
	head     uint64 //used for List
	tail     uint64 //used for List
}
type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}
type SetInternalKey struct {
	key     []byte
	version int64
	member  []byte
}
type ListInternalKey struct {
	key     []byte
	version int64
	index   uint64
}
type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64
}

func (zset *zsetInternalKey) encode() []byte {

	buf := make([]byte, len(zset.key)+len(zset.member)+8)
	// key
	var index = 0
	copy(buf[index:index+len(zset.key)], zset.key)
	index += len(zset.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zset.version))
	index += 8

	// member
	copy(buf[index:], zset.member)

	return buf
}
func (zset *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zset.score)
	buf := make([]byte, len(zset.key)+len(zset.member)+len(scoreBuf)+8+4)
	// key
	var index = 0
	copy(buf[index:index+len(zset.key)], zset.key)
	index += len(zset.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zset.version))
	index += 8

	// score
	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)
	// member
	copy(buf[index:index+len(zset.member)], zset.member)
	index += len(zset.member)

	//member size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(zset.member)))
	return buf
}

func (list *ListInternalKey) encode() []byte {
	buf := make([]byte, len(list.key)+8+8)
	//key
	var index = 0
	index += copy(buf[index:], list.key)
	//version
	binary.LittleEndian.PutUint64(buf[index:], uint64(list.version))
	index += 8
	//index
	binary.LittleEndian.PutUint64(buf[index:], list.index)
	return buf
}

func (meta *metadata) encodeMetaData() []byte {
	var size = maxMetaDataSize
	if meta.dataType == List {
		size = maxMetaDataSizeForList
	}
	buf := make([]byte, size)
	buf[0] = meta.dataType
	var index = 1
	index += binary.PutVarint(buf[index:], meta.expire)
	index += binary.PutVarint(buf[index:], meta.version)
	index += binary.PutUvarint(buf[index:], uint64(meta.size))
	if meta.dataType == List {
		index += binary.PutUvarint(buf[index:], meta.head)
		index += binary.PutUvarint(buf[index:], meta.tail)
	}
	return buf[:index]
}
func decodeMetaData(buf []byte) *metadata {
	dataType := buf[0]
	expire, n1 := binary.Varint(buf[1:])
	version, n2 := binary.Varint(buf[n1+1:])
	size, n3 := binary.Uvarint(buf[n1+n2+1:])
	if dataType == List {
		head, n4 := binary.Uvarint(buf[n1+n2+n3+1:])
		tail, _ := binary.Uvarint(buf[n1+n2+n3+n4+1:])
		return &metadata{
			dataType: dataType,
			expire:   expire,
			version:  version,
			size:     uint32(size),
			head:     head,
			tail:     tail,
		}
	}
	return &metadata{
		dataType: dataType,
		expire:   expire,
		version:  version,
		size:     uint32(size),
	}
}
func (hashkey *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hashkey.key)+len(hashkey.field)+8)
	// key
	var index = 0
	copy(buf[index:index+len(hashkey.key)], hashkey.key)
	index += len(hashkey.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hashkey.version))
	index += 8

	// field
	copy(buf[index:], hashkey.field)

	return buf
}
func (setKey *SetInternalKey) encode() []byte {
	buf := make([]byte, len(setKey.key)+len(setKey.member)+8+4)
	// key
	var index = 0
	copy(buf[index:index+len(setKey.key)], setKey.key)
	index += len(setKey.key)

	// version
	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(setKey.version))
	index += 8

	// member
	copy(buf[index:index+len(setKey.member)], setKey.member)
	index += len(setKey.member)
	//member Size
	binary.LittleEndian.PutUint32(buf[index:], uint32(len(setKey.member)))

	return buf
}
