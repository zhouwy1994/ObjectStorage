package locate

import (
	"encoding/json"
	"fmt"
	rs"github.com/zhouwy1994/ObjectStorage/Unit5/interfaceService/module/reedsolomon"
	"github.com/zhouwy1994/ObjectStorage/Unit5/storageService/module/rabbitmq"
	"log"
	"os"
	"time"
)

type LocateMessage struct {
	Addr string
	Id int
}

func Locate(name string) (locateInfo map[int]string) {
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

	for i := 0 ; i < rs.ALL_SHAREDS; i++ {
		msg := <- ch
		if len(msg.Body) == 0 {
			return
		}

		var info LocateMessage
		fmt.Printf("%s", msg.Body)
		err := json.Unmarshal(msg.Body, &info)
		if err != nil {
			log.Println(err)
			continue
		}

		locateInfo[info.Id] = info.Addr
	}

	return
}

func IsExist(name string) bool {
	return len(Locate(name)) >= rs.DATA_SHAREDS
}