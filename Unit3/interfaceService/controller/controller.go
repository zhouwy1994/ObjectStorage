package controller

import (
	bytes2 "bytes"
	"encoding/json"
	"github.com/zhouwy1994/ObjectStorage/Unit3/interfaceService/controller/heartbeat"
	"github.com/zhouwy1994/ObjectStorage/Unit3/interfaceService/controller/locate"
	"github.com/zhouwy1994/ObjectStorage/Unit3/interfaceService/module/elasticsearch"
	"github.com/zhouwy1994/ObjectStorage/Unit3/interfaceService/module/objectstream"
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

	s := heartbeat.ChooseRandomDataServer()
	if s == "" {
		log.Println("Not Available DataServer")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	putStream := objectstream.NewPutStream(s, hash)
	io.Copy(putStream, r.Body)
	err = putStream.Close()

	if err != nil {
		log.Println("Request DataServer Failed:", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = elasticsearch.AddVersion(objName, int64(size), hash)
	if err != nil {
		log.Println("ElasticSearch Add Version Fail")
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

	ds := locate.Locate(meta.Hash)
	if ds == "" {
		log.Println("Object Not Find")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	getStream, err := objectstream.NewGetStream(ds, meta.Hash)
	if err != nil {
		log.Println("Request DataServer Failed")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	io.Copy(w, getStream)
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
	if ds == "" {
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
