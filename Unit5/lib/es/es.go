package es

import (
	"encoding/json"
	"fmt"
	gjs "github.com/bitly/go-simplejson"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)
type Metadate struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
	Size    int64  `json:"size"`
	Hash    string `json:"hash"`
}

func getMetadata(name string, version int) (meta Metadate, err error) {
	esUrl := fmt.Sprintf(`http://%s/metadata/_doc/%s_%d/_source`,
		os.Getenv("ES_SERVER"), name, version)
	resp, err := http.Get(esUrl)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return meta, fmt.Errorf(`fail to get metadata:%d`, resp.StatusCode)
	}

	body, _ := ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, &meta)

	return
}

func SearchLatestVersion(name string) (meta Metadate, err error) {
	esUrl := fmt.Sprintf(`http://%s/metadata/_search?q=name:%s&sort=version:desc&size=1`,
		os.Getenv("ES_SERVER"), name)
	resp, err := http.Get(esUrl)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return meta, fmt.Errorf(`fail to get laste version:%d`, resp.StatusCode)
	}

	//io.Copy(os.Stdout, resp.Body)
	js, _ := gjs.NewFromReader(resp.Body)
	ss, _ := js.Get("hits").Get("hits").GetIndex(0).Get("_source").MarshalJSON()
	json.Unmarshal(ss, &meta)

	return
}

func GetMetadata(name string, version int) (Metadate, error) {
	if version == 0 {
		return SearchLatestVersion(name)
	}

	return getMetadata(name, version)
}

func PutMetadata(name string, version int, size int64, hash string) error {
	esData := fmt.Sprintf(`{"name":"%s","version":%d,"size":%d,"hash":"%s"}`,
		name, version, size, hash)
	esUrl := fmt.Sprintf(`http://%s/metadata/_doc/%s_%d?op_type=create`,
		os.Getenv("ES_SERVER"), name, version)
	esRequest, _ := http.NewRequest("PUT", esUrl, strings.NewReader(esData))
	esRequest.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(esRequest)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return PutMetadata(name, version+1, size, hash)
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf(`fail to put metadata:%d`, resp.StatusCode)
	}

	return nil
}

func AddVersion(name string, size int64, hash string) error {
	meta, err := SearchLatestVersion(name)
	if err != nil {
		return err
	}

	return PutMetadata(name, meta.Version+1, size, hash)
}

func SearchAllVersions(name string, from, size int) (metas []Metadate, err error) {
	esUrl := fmt.Sprintf(`http://%s/metadata/_search?q=name:%s&sort=version:desc&size=%d`,
		os.Getenv("ES_SERVER"), name, size)
	resp, err := http.Get(esUrl)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return metas, fmt.Errorf(`fail to get all meta:%d`, resp.StatusCode)
	}

	js, _ := gjs.NewFromReader(resp.Body)
	length := len(js.Get("hits").Get("hits").MustArray())
	for i := 0; i < length; i++ {
		ss, _ := js.Get("hits").Get("hits").GetIndex(i).Get("_source").MarshalJSON()
		meta := Metadate{}
		json.Unmarshal(ss, &meta)
		metas = append(metas, meta)
	}

	return
}