package locate

import (
	"fmt"
	"log"
	"os"
	"github.com/zhouwy1994/ObjectStorage/Unit4/storageService/module/rabbitmq"
	"path/filepath"
	"strconv"
	"sync"
)

var objects = make(map[string]bool)
var mutex sync.Mutex

func StartLocate() {
	rbAddr := os.Getenv("RABBITMQ_ADDR")
	if rbAddr == "" {
		rbAddr = "127.0.0.1:3456"
	}

	q,err := rabbitmq.New(fmt.Sprintf("amqp://zhouwy:admin@%s/admin", rbAddr))
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	listenAddr := os.Getenv("LISTEN_ADDR")
	if listenAddr == "" {
		log.Fatal("Listen Addr NULL")
	}

	err = q.Bind("dataServers")
	if err != nil {
		log.Fatal(err)
	}

	storagePath :=  os.Getenv("STORAGE_PATH")
	if storagePath == "" {
		log.Fatal("Invalid Storage Path")
	}

	ch,err := q.Consume()
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg,isClose := <- ch
		if !isClose {
			log.Fatal("channel already closed")
		}

		object,_ := strconv.Unquote(string(msg.Body))
		if (Local(object)) {
			q.Send(msg.ReplyTo, listenAddr)
		}
	}
}

func Local(hash string) bool {
	mutex.Lock()
	defer mutex.Unlock()

	return objects[hash]
}

func Add(hash string) {
	mutex.Lock()
	defer mutex.Unlock()
	objects[hash] = true
}

func Del(hash string) {
	mutex.Lock()
	defer mutex.Unlock()
	delete (objects, hash)
}

func CollectObject() {
	files,_ := filepath.Glob(os.Getenv("STORAGE_PATH") + "*")
	for i := range files {
		hash := filepath.Base(files[i])
		objects[hash] = true
	}
}



