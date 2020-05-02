package objectstream

import (
	"fmt"
	"io"
	"net/http"
)

type PutStream struct {
	writer *io.PipeWriter
	ch chan error
}

func NewPutStream(server, object string) *PutStream {
	reader,writer := io.Pipe()
	ech := make(chan error)

	go func() {
		request,_ := http.NewRequest("PUT", `http://` + server + `/objects/` +object, reader)
		resp,err := http.DefaultClient.Do(request)
		if err == nil && resp.StatusCode != http.StatusOK {
			err = fmt.Errorf("dataServer return error code:%d", resp.StatusCode)
		}

		ech <- err
	}()

	return &PutStream{writer: writer, ch: ech}
}

func (w *PutStream) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *PutStream) Close() error {
	w.writer.Close()
	return <- w.ch
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
