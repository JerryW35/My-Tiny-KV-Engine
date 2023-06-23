package http

import (
	"KVstore"
	"encoding/json"
	"log"
	"net/http"
	"os"
)

var db *KVstore.DB

func init() {
	var err error
	config := KVstore.DefaultConfigs
	dir, _ := os.MkdirTemp("./", "kv-http")
	config.DirPath = dir
	db, err = KVstore.Open(config)
	if err != nil {
		panic(err)
	}
}

// curl -X POST localhost:8088/mykv/put -d '{"name1":"value1","name2":"value2"}'
func handlePut(writer http.ResponseWriter, request *http.Request) {
	// 1. get key and value from request
	if request.Method != http.MethodPost {
		http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	// parse the params
	var data map[string]string
	if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest)
		return
	}
	// 2. put key and value to db
	for key, val := range data {
		err := db.Put([]byte(key), []byte(val))
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			log.Println("failed to put key:", key, "val:", val)
			return
		} else {
			log.Println("put key:", key, "val:", val)
		}
	}
	return
}

// curl "localhost:8088/mykv/get?key=name"
func handleGet(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	val, err := db.Get([]byte(key))
	if err != nil && err != KVstore.ErrorKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Println("failed to get key:", key)
		return
	} else {
		log.Println("get key:", key, "val:", string(val))
	}
	// return value to client
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(map[string]string{key: string(val)})

}
func handleDelete(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodDelete {
		http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	key := request.URL.Query().Get("key")
	err := db.Delete([]byte(key))
	if err != nil && err != KVstore.ErrorKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError)
		log.Println("failed to delete key:", key)
		return
	} else {
		log.Println("delete key:", key)
	}
	// return value to client
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}
func HandleListKeys(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")
	var data []string
	for _, key := range keys {
		data = append(data, string(key))
	}
	_ = json.NewEncoder(writer).Encode(data)

}
func HandleStat(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.Error(writer, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}
func main() {
	http.HandleFunc("/mykv/get", handleGet)
	http.HandleFunc("/mykv/put", handlePut)
	http.HandleFunc("/mykv/delete", handleDelete)
	http.HandleFunc("/mykv/listkeys", HandleListKeys)
	http.HandleFunc("/mykv/stat", HandleStat)

	// start http server
	_ = http.ListenAndServe("localhost:8088", nil)

}
