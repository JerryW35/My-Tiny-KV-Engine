package main

import (
	"KVstore"
	"KVstore/Redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "localhost:6380"

type MyServer struct {
	dbs         map[int]*Redis.RedisDataStructure
	redisServer *redcon.Server
	mu          sync.RWMutex
}

func main() {
	// start redis server
	redisDataStructure, err := Redis.NewRedisDataStructure(KVstore.DefaultConfigs)
	if err != nil {
		panic(err)
	}
	//init  server
	myKVServer := MyServer{
		dbs: make(map[int]*Redis.RedisDataStructure),
	}
	myKVServer.dbs[0] = redisDataStructure

	// init a redis server
	myKVServer.redisServer = redcon.NewServer(addr, execClientCommand, myKVServer.accept, myKVServer.Close)
	myKVServer.listen()

}

func (server *MyServer) listen() {
	log.Println("start redis server at ", addr)
	_ = server.redisServer.ListenAndServe()
}

func (server *MyServer) accept(conn redcon.Conn) bool {
	cli := new(myClient)
	server.mu.Lock()
	defer server.mu.Unlock()
	cli.server = server
	cli.db = server.dbs[0]
	conn.SetContext(cli)
	return true
}

func (server *MyServer) Close(conn redcon.Conn, err error) {
	for _, db := range server.dbs {
		err := db.Close()
		if err != nil {
			return
		}
	}
	_ = server.redisServer.Close()
}

//func main() {
//	conn, err := net.Dial("tcp", "localhost:6379")
//	if err != nil {
//		panic(err)
//	}
//	// send a command
//	cmd := "set name terry\r\n"
//	conn.Write([]byte(cmd))
//	// read the response
//	reader := bufio.NewReader(conn)
//	line, err := reader.ReadString('\n')
//	if err != nil {
//		panic(err)
//	}
//	println(line)
//}
