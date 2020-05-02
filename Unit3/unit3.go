package Unit3

import (
	heartbeat2 "github.com/zhouwy1994/ObjectStorage/Unit3/interfaceService/controller/heartbeat"
	heartbeat1 "github.com/zhouwy1994/ObjectStorage/Unit3/storageService/controller/heartbeat"
	locate1 "github.com/zhouwy1994/ObjectStorage/Unit3/storageService/controller/locate"
	"log"
	"net/http"
	"os"

	controller2 "github.com/zhouwy1994/ObjectStorage/Unit3/interfaceService/controller"
	controller1 "github.com/zhouwy1994/ObjectStorage/Unit3/storageService/controller"
)

func StartStorageService() {
	go heartbeat1.StartHeartbeat()
	go locate1.StartLocate()

	http.DefaultServeMux.HandleFunc("/objects/", stroageRouteDistributor)
	listenAddr := os.Getenv("LISTEN_ADDR")
	if len(listenAddr) < 1 {
		listenAddr = ":8080"
	}

	server := http.Server{Addr: listenAddr, Handler: http.DefaultServeMux}
	log.Fatal(server.ListenAndServe())
}

func StartInterfaceService() {
	go heartbeat2.StartHeartbeatListen()

	http.DefaultServeMux.HandleFunc("/objects/", interfaceRouteDistributor)
	http.DefaultServeMux.HandleFunc("/locate/", interfaceLocateRouteDistributor)
	http.DefaultServeMux.HandleFunc("/versions/", controller2.Versions)
	listenAddr := os.Getenv("LISTEN_ADDR")
	if len(listenAddr) < 1 {
		listenAddr = ":8080"
	}

	server := http.Server{Addr: listenAddr, Handler: http.DefaultServeMux}
	log.Fatal(server.ListenAndServe())
}

func stroageRouteDistributor(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		controller1.GetObject(w, r)
		return
	}

	if r.Method == http.MethodPut {
		controller1.PutObject(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func interfaceRouteDistributor(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		controller2.GetObject(w, r)
		return
	}

	if r.Method == "PUT" {
		controller2.PutObject(w, r)
		return
	}

	if r.Method == http.MethodDelete {
		controller2.DeleteObject(w, r)
		return
	}


	w.WriteHeader(http.StatusMethodNotAllowed)
}

func interfaceLocateRouteDistributor(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		controller2.LocateObject(w, r)
		return
	}

	w.WriteHeader(http.StatusMethodNotAllowed)
}