package controller

import (
	"encoding/json"
	"github.com/zhouwy1994/ObjectStorage/Unit2/interfaceService/controller/heartbeat"
	"github.com/zhouwy1994/ObjectStorage/Unit2/interfaceService/controller/locate"
	"github.com/zhouwy1994/ObjectStorage/Unit2/interfaceService/module/objectstream"
	"io"
	"log"
	"net/http"
	"strings"
)

func PutObject(w http.ResponseWriter,r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(objName) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	s := heartbeat.ChooseRandomDataServer()
	if s == "" {
		log.Println("Not Available DataServer")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	putStream := objectstream.NewPutStream(s, objName)
	io.Copy(putStream, r.Body)
	err := putStream.Close()

	if err != nil {
		log.Println("Request DataServer Failed")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func GetObject(w http.ResponseWriter,r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(objName) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds := locate.Locate(objName)
	if ds == "" {
		log.Println("Object Not Find")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	getStream,err := objectstream.NewGetStream(ds, objName)
	if err != nil {
		log.Println("Request DataServer Failed")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	io.Copy(w, getStream)
}

func LocateObject(w http.ResponseWriter,r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(objName) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds := locate.Locate(objName)
	if ds == "" {
		log.Println("Object Not Find")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	body,_ := json.Marshal(ds)
	w.Write(body)
}
