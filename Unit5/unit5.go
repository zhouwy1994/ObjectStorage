package Unit5

import (
	heartbeat2 "github.com/zhouwy1994/ObjectStorage/Unit5/interfaceService/controller/heartbeat"
	heartbeat1 "github.com/zhouwy1994/ObjectStorage/Unit5/storageService/controller/heartbeat"
	locate1 "github.com/zhouwy1994/ObjectStorage/Unit5/storageService/controller/locate"
	"log"
	"net/http"
	"os"

	controller2 "github.com/zhouwy1994/ObjectStorage/Unit5/interfaceService/controller"
	controller1 "github.com/zhouwy1994/ObjectStorage/Unit5/storageService/controller"
)

func StartStorageService() {
	locate1.CollectObject()
	go heartbeat1.StartHeartbeat()
	go locate1.StartLocate()

	http.DefaultServeMux.HandleFunc("/objects/", stroageRouteDistributor)
	http.DefaultServeMux.HandleFunc("/temp/", stroageTempRouteDistributor)
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

	w.WriteHeader(http.StatusMethodNotAllowed)
}

func stroageTempRouteDistributor(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		controller1.PostTempObject(w, r)
		return
	}

	if r.Method == http.MethodPut {
		controller1.PutTempObject(w, r)
		return
	}

	if r.Method == http.MethodPatch {
		controller1.PatchTempObject(w, r)
		return
	}

	if r.Method == http.MethodDelete {
		controller1.DeleteTempObject(w, r)
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