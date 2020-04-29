package controller

import (
	"bytes"
	"net/http"
	"os"
	"strings"

	"github.com/zhouwy1994/ObjectStorage/Unit1/module"
)

const defaultStoragePath = `./StoragePath/Unit2/`

func PutObject(w http.ResponseWriter,r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(objName) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	storagePath := os.Getenv("STORAGE_PATH")
	if len(storagePath) < 1 {
		storagePath = defaultStoragePath
	}

	err := module.StorageObject(storagePath + objName,r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func GetObject(w http.ResponseWriter,r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(objName) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	storagePath := os.Getenv("STORAGE_PATH")
	if len(storagePath) < 1 {
		storagePath = defaultStoragePath
	}

	buffer := bytes.NewBuffer(nil)
	err := module.LoadObject(storagePath + objName,buffer)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(200)
	w.Write(buffer.Bytes())
}
