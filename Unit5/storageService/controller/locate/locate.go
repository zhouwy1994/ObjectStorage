package locate

import (
	"fmt"
	"log"
	"os"
	"github.com/zhouwy1994/ObjectStorage/Unit5/storageService/module/rabbitmq"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var objects = make(map[string]int)
var mutex sync.Mutex

type LocateMessage struct {
	Addr string
	Id int
}

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
		id := Local(object)
		if id != -1 {
			q.Send(msg.ReplyTo, LocateMessage{Addr: listenAddr, Id: id})
		}
	}
}

func Local(hash string) int {
	mutex.Lock()
	defer mutex.Unlock()

	id,ok := objects[hash]
	if !ok {
		return -1
	}

	return id
}

func Add(hash string, id int) {
	mutex.Lock()
	defer mutex.Unlock()
	objects[hash] = id
}

func Del(hash string) {
	mutex.Lock()
	defer mutex.Unlock()
	delete (objects, hash)
}

func CollectObject() {
	files,_ := filepath.Glob(os.Getenv("STORAGE_PATH") + "*")
	for i := range files {
		file := strings.Split(filepath.Base(files[i]), ".")
		if len(file) != 3 {
			panic(files[i])
		}

		hash := file[0]
		id,e := strconv.Atoi(file[i])
		if e != nil {
			panic(e)
		}

		objects[hash] = id
	}
}



