package heartbeat

import (
	"fmt"
	"github.com/zhouwy1994/ObjectStorage/Unit5/storageService/module/rabbitmq"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

var dataServers = make(map[string] time.Time)
var mutex sync.Mutex

func StartHeartbeatListen() {
	rbAddr := os.Getenv("RABBITMQ_ADDR")
	if rbAddr == "" {
		rbAddr = "127.0.0.1:3456"
	}

	q,err := rabbitmq.New(fmt.Sprintf("amqp://zhouwy:admin@%s/admin", rbAddr))
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	err = q.Bind("apiServers")
	if err != nil {
		log.Fatalln(err)
	}

	ch,err := q.Consume()
	if err != nil {
		log.Fatalln(err)
	}

	go removeExpireDataServer()

	for {
		msg,isClose := <- ch
		if !isClose {
			return
		}

		s,_ := strconv.Unquote(string(msg.Body))
		mutex.Lock()
		dataServers[s] = time.Now()
		mutex.Unlock()
	}

}

func removeExpireDataServer() {
	for {
		time.Sleep(5 * time.Second)
		mutex.Lock()
		for s, t := range dataServers {
			if t.Add(10 * time.Second).Before(time.Now()) {
				delete(dataServers, s)
			}
		}
		mutex.Unlock()
	}
}

func GetDataServers() []string {
	mutex.Lock()
	defer mutex.Unlock()

	dss := make([]string, 0)

	for s,_ := range dataServers {
		dss = append(dss, s)
	}

	return dss
}

func ChooseRandomDataServer(n int, exclude map[int]string) (ds []string) {
	candidatas := make([]string ,0)
	reverseExcludeMap := make(map[string]int)

	for id,addr := range exclude {
		reverseExcludeMap[addr] = id
	}

	servers := GetDataServers()

	for i := range servers {
		s := servers[i]
		_,excluded := reverseExcludeMap[s]
		if !excluded {
			candidatas = append(candidatas, s)
		}
	}

	length := len(candidatas)
	if length < n {
		return
	}

	p := rand.Perm(length)

	for i := 0; i < n; i++ {
		ds = append(ds, candidatas[p[i]])
	}

	return
}