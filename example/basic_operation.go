package example

import (
	"KVstore"
	"fmt"
)

func main() {
	configs := KVstore.DefaultConfigs
	configs.DirPath = "./test_data/"
	db, err := KVstore.Open(configs)
	if err != nil {
		panic(err)
	}
	//err = db.Put([]byte("name"), []byte("jerry"))
	//if err != nil {
	//	panic(err)
	//}
	val, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
	}
	fmt.Println("val=", string(val))

}
