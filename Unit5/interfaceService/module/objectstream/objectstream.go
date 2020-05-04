package objectstream

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
)

type TempPutStream struct {
	Server string
	Uuid string
}

func NewTempPutStream(server, object string, size int64) (*TempPutStream, error) {
	request,_ := http.NewRequest("POST", fmt.Sprintf(`http://%s/temp/%s`,
		server, object), nil)
	request.Header.Set("Size", strconv.Itoa(int(size)))
	resp,err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("new create temp uuid fail:%d", resp.StatusCode)
	}

	uuid,_ := ioutil.ReadAll(resp.Body)

	return &TempPutStream{Server: server, Uuid: string(uuid)},nil
}

func (w *TempPutStream) Write(b []byte) (int,error) {
	req,_ := http.NewRequest("PATCH", fmt.Sprintf(`http://%s/temp/%s`,
		w.Server, w.Uuid), bytes.NewBuffer(b))
	resp,err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("patch write failed:%d", resp.StatusCode)
	}

	return len(b),nil
}

func (w *TempPutStream)Commit(ok bool) error {
	method := "DELETE"
	if ok {
		method = "PUT"
	}

	req,_ := http.NewRequest(method, fmt.Sprintf(`http://%s/temp/%s`,
		w.Server, w.Uuid), nil)
	resp,err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return fmt.Errorf("commit failed")
	}

	return nil
}


type GetStream struct {
	reader io.Reader
}

func NewGetStream(server, object string) (*GetStream,error) {
	resp,err := http.Get(`http://`+server+`/objects/`+object)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Failed to request url")
	}

	return &GetStream{reader: resp.Body},nil
}

func(s *GetStream) Read(p []byte) (n int, err error) {
	return s.reader.Read(p)
}
