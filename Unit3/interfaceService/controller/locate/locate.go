package locate

import (
	"fmt"
	"github.com/zhouwy1994/ObjectStorage/Unit3/storageService/module/rabbitmq"
	"log"
	"os"
	"strconv"
	"time"
)

func Locate(name string) string {
	rbAddr := os.Getenv("RABBITMQ_ADDR")
	if rbAddr == "" {
		rbAddr = "127.0.0.1:3456"
	}

	q,err := rabbitmq.New(fmt.Sprintf("amqp://zhouwy:admin@%s/admin", rbAddr))
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	err = q.Publish("dataServers", name)
	if err != nil {
		log.Fatalln(err)
	}

	ch,err := q.Consume()
	if err != nil {
		log.Fatalln(err)
	}

	time.AfterFunc(2 * time.Second, func() {
		q.Close()
	})

	msg := <- ch
	s,_ := strconv.Unquote(string(msg.Body))

	return s
}

func IsExist(name string) bool {
	return Locate(name) != ""
}