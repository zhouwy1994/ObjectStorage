package locate

import (
	"fmt"
	"log"
	"os"
	"github.com/zhouwy1994/ObjectStorage/Unit2/storageService/module/rabbitmq"
	"strconv"
)

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
		if (locate(storagePath + object)) {
			q.Send(msg.ReplyTo, listenAddr)
		}
	}
}

func locate(fullpath string) bool {
	_,err := os.Stat(fullpath)
	return !os.IsNotExist(err)
}