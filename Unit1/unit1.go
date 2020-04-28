package Unit1

import (
	"log"
	"net/http"
	"os"

	"github.com/zhouwy1994/ObjectStorage/Unit1/controller"
)

func StartStorageService() {
	http.DefaultServeMux.HandleFunc("/objects/", routeDistributor)
	listenAddr := os.Getenv("LISTEN_ADDR_UNIT1")
	if len(listenAddr) < 1 {
		listenAddr = ":8080"
	}

	server := http.Server{Addr: listenAddr, Handler: http.DefaultServeMux}
	log.Fatal(server.ListenAndServe())
}

func routeDistributor(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		controller.GetObject(w, r)
		return
	}

	if r.Method == "PUT" {
		controller.PutObject(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}