package main

import (
	"github.com/zhouwy1994/ObjectStorage/Unit5/apiServer/heartbeat"
	"github.com/zhouwy1994/ObjectStorage/Unit5/apiServer/locate"
	"github.com/zhouwy1994/ObjectStorage/Unit5/apiServer/objects"
	"github.com/zhouwy1994/ObjectStorage/Unit5/apiServer/versions"
	"log"
	"net/http"
	"os"
)

func main() {
	go heartbeat.ListenHeartbeat()
	http.HandleFunc("/objects/", objects.Handler)
	http.HandleFunc("/locate/", locate.Handler)
	http.HandleFunc("/versions/", versions.Handler)
	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))
}
