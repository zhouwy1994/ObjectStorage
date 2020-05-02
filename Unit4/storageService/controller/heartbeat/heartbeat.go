package heartbeat

import (
	"fmt"
	"github.com/zhouwy1994/ObjectStorage/Unit4/storageService/module/rabbitmq"
	"log"
	"os"
	"time"
)

func StartHeartbeat() {
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

	for {
		err := q.Publish("apiServers", listenAddr)
		if err != nil {
			log.Fatalln(err)
		}
		time.Sleep(5 * time.Second)
	}
}