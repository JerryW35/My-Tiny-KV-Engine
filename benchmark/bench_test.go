package benchmark

import (
	"KVstore"
	"KVstore/utils"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"testing"
	"time"
)

var db *KVstore.DB

func init() {
	var err error
	config := KVstore.DefaultConfigs
	dir, _ := os.MkdirTemp("./", "tests")
	config.DirPath = dir
	db, err = KVstore.Open(config)
	if err != nil {
		panic(err)
	}
}
func Benchmark_Put(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}
}
func Benchmark_Get(b *testing.B) {
	for i := 0; i < 10000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024))
		assert.Nil(b, err)
	}

	rand.Seed(time.Now().UnixNano())
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != KVstore.ErrorKeyNotFound {
			b.Fatal(err)
		}
	}
}
func Benchmark_Delete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := db.Delete(utils.GetTestKey(rand.Int()))
		assert.Nil(b, err)
	}
}
