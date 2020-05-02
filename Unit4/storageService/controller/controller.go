package controller

import (
	"bytes"
	"encoding/json"
	locate1 "github.com/zhouwy1994/ObjectStorage/Unit4/storageService/controller/locate"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/satori/go.uuid"
	"github.com/zhouwy1994/ObjectStorage/Unit1/module"
)

const defaultStoragePath = `./StoragePath/Unit4/`

type TempInfo struct {
	Uuid string
	Name string
	Size int64
}

func PostTempObject(w http.ResponseWriter,r *http.Request) {
	defer r.Body.Close()
	uuid := uuid.NewV4().String()
	size,_ := strconv.ParseInt(r.Header.Get("size"), 10, 64)
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	objName := strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(objName) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if size < 1 {
		log.Printf("SIZE equal 0")
		return
	}
	
	f := TempInfo{Uuid: uuid, Name: objName, Size: size}
	err := f.writeToFile()
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	file,_ := os.Create(os.Getenv("STORAGE_PATH") + "/temp/" + f.Uuid + ".dat")
	defer file.Close()
	io.WriteString(w, uuid)
}

func (f* TempInfo)writeToFile() error {
	file,err := os.Create(os.Getenv("STORAGE_PATH") + "/temp/" + f.Uuid)
	if err != nil {
		return err
	}
	defer file.Close()
	
	b,_ := json.Marshal(f)
	file.Write(b)

	return nil
}

func PutTempObject(w http.ResponseWriter,r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	uuid := strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(uuid) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	tempInfo,err := readFromFile(uuid)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	infoFile := os.Getenv("STORAGE_PATH") + "/temp/" + uuid
	dataFile := infoFile + ".dat"
	df,_ := os.Open(dataFile)
	dfStat,_ := df.Stat()

	os.Remove(infoFile)
	dsize := dfStat.Size()

	df.Close()

	if tempInfo.Size != dsize {
		os.Remove(dataFile)
		log.Println("data size not equal info")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = os.Rename(dataFile, os.Getenv("STORAGE_PATH") + tempInfo.Name)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	locate1.Add(tempInfo.Name)


}

func PatchTempObject(w http.ResponseWriter,r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	uuid := strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(uuid) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	tempInfo,err := readFromFile(uuid)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	infoFile := os.Getenv("STORAGE_PATH") + "/temp/" + uuid
	dataFile := infoFile + ".dat"

	df,err := os.OpenFile(dataFile, os.O_WRONLY | os.O_APPEND, 0)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer df.Close()

	io.Copy(df, r.Body)

	info,_ := df.Stat()
	if info.Size() > tempInfo.Size {
		os.Remove(dataFile)
		os.Remove(infoFile)
		log.Println("size not equeal")
		w.WriteHeader(http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusOK)
}

func readFromFile(uuid string) (*TempInfo,error){
	data,_ := ioutil.ReadFile(os.Getenv("STORAGE_PATH") + "/temp/" + uuid)
	i := TempInfo{}
	json.Unmarshal(data, &i)

	return &i,nil
}

func DeleteTempObject(w http.ResponseWriter,r *http.Request) {
	defer r.Body.Close()
	// 由于Mux路由的地址为/objects/,那么Split的值至少是3个
	uuid := strings.Split(r.URL.EscapedPath(),"/")[2]
	if len(uuid) < 1 {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	infoFile := os.Getenv("STORAGE_PATH") + "/temp/" + uuid
	dataFile := infoFile + ".dat"

	os.Remove(infoFile)
	os.Remove(dataFile)
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
