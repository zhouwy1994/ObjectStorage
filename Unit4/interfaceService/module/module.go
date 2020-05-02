package module

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/zhouwy1994/ObjectStorage/Unit4/interfaceService/controller/heartbeat"
	"github.com/zhouwy1994/ObjectStorage/Unit4/interfaceService/controller/locate"
	"github.com/zhouwy1994/ObjectStorage/Unit4/interfaceService/module/objectstream"
	"io"
	"net/http"
)

func StorageObject(r io.Reader, hash string, size int64) (int, error) {
	if locate.IsExist(hash) {
		return http.StatusOK,nil
	}

	stream,err := putStream(hash, size)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	reader := io.TeeReader(r, stream)
	d := calcHah(reader)
	if d != hash {
		stream.Commit(false)
		return http.StatusBadRequest,fmt.Errorf(`hash not equeal d`)
	}

	stream.Commit(true)
	return http.StatusOK,nil


}

func putStream(hash string, size int64) (stream *objectstream.TempPutStream, err error) {
	s := heartbeat.ChooseRandomDataServer()
	if s == "" {
		return nil, fmt.Errorf("Not Available DataServer")
	}

	return objectstream.NewTempPutStream(s, hash, size)
}

func calcHah(reader io.Reader) string {
	hasher := sha256.New()
	io.Copy(hasher, reader)
	return hex.EncodeToString(hasher.Sum(nil))
}
