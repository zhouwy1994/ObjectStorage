package main

import (
	"github.com/zhouwy1994/ObjectStorage/Unit5/dataServer/heartbeat"
	"github.com/zhouwy1994/ObjectStorage/Unit5/dataServer/locate"
	"github.com/zhouwy1994/ObjectStorage/Unit5/dataServer/objects"
	"github.com/zhouwy1994/ObjectStorage/Unit5/dataServer/temp"
	"log"
	"net/http"
	"os"
)

func main() {
	locate.CollectObjects()
	go heartbeat.StartHeartbeat()
	go locate.StartLocate()
	http.HandleFunc("/objects/", objects.Handler)
	http.HandleFunc("/temp/", temp.Handler)
	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))
}
