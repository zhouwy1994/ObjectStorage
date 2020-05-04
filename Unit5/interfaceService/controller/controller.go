package controller

import (
	bytes2 "bytes"
	"encoding/json"
	"fmt"
	"github.com/zhouwy1994/ObjectStorage/Unit5/interfaceService/controller/heartbeat"
	"github.com/zhouwy1994/ObjectStorage/Unit5/interfaceService/controller/locate"
	"github.com/zhouwy1994/ObjectStorage/Unit5/interfaceService/module"
	"github.com/zhouwy1994/ObjectStorage/Unit5/interfaceService/module/elasticsearch"
	rs "github.com/zhouwy1994/ObjectStorage/Unit5/interfaceService/module/reedsolomon"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
)

func PutObject(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(), "/")[2]
	hash := r.Header.Get("digest")
	size, err := strconv.Atoi(r.Header.Get("content-length"))
	if len(objName) < 1 || len(hash) == 0 || err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	c,err := module.StorageObject(r.Body, hash, int64(size))
	if err != nil || c != http.StatusOK{
		w.WriteHeader(c)
		log.Println(err)
		return
	}

	e := elasticsearch.AddVersion(objName, int64(size), hash)
	if e != nil {
		log.Println(e)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func GetObject(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(), "/")[2]
	versionId := r.URL.Query()["version"]
	if len(objName) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	version := 0
	var err error
	if len(versionId) != 0 {
		version, err = strconv.Atoi(versionId[0])
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	meta, err := elasticsearch.GetMetadata(objName, version)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if meta.Hash == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	stream,err := GetStream(meta.Hash, meta.Size)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	_,err = io.Copy(w, stream)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	stream.Close()

}

func DeleteObject(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(), "/")[2]
	if len(objName) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	meta, err := elasticsearch.SearchLatestVersion(objName)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = elasticsearch.PutMetadata(objName, meta.Version+1, 0, "")
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func LocateObject(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(), "/")[2]
	if len(objName) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	ds := locate.Locate(objName)
	if len(ds) < rs.DATA_SHAREDS {
		log.Println("Object Not Find")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	body, _ := json.Marshal(ds)
	w.Write(body)
}

func Versions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	object := strings.Split(r.URL.EscapedPath(), "/")[2]
	from, size := 0, 1000
	metas, err := elasticsearch.SearchAllVersion(object, from, size)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	buffer := bytes2.NewBuffer(nil)
	for i := range metas {
		b, _ := json.Marshal(metas[i])
		buffer.Write(b)
		buffer.WriteString("\n")
	}

	w.Write(buffer.Bytes())
}

func GetStream(hash string, size int64) (*rs.RSGetStream, error) {
	locateInfo := locate.Locate(hash)
	if len(locateInfo) < rs.DATA_SHAREDS {
		return nil, fmt.Errorf(`object locate failed`)
	}

	dataServer := make([]string, 0)
	if len(locateInfo) != rs.ALL_SHAREDS {
		dataServer = heartbeat.ChooseRandomDataServer(rs.ALL_SHAREDS - len(locateInfo), locateInfo)
	}

	return rs.NewRSGetStream(locateInfo, dataServer, hash, size)
}