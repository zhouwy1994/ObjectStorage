package rs

import (
	"fmt"
	"github.com/klauspost/reedsolomon"
	"github.com/zhouwy1994/ObjectStorage/Unit5/interfaceService/module/objectstream"
	"io"
)

const (
	DATA_SHAREDS = 4
	PARITY_SHAREDS = 2
	ALL_SHAREDS = DATA_SHAREDS + PARITY_SHAREDS
	BLOCK_PRE_SHARED = 8000
	BLOCK_SIZE = BLOCK_PRE_SHARED * DATA_SHAREDS
)

type RSPutStream struct {
	*encoder
}

func NewRSPutStream(servers []string, hash string, size int64) (*RSPutStream, error) {
	if len(servers) != ALL_SHAREDS {
		return nil, fmt.Errorf("dataserver number mismatch")
	}

	perShared := (size + DATA_SHAREDS -1) / DATA_SHAREDS
	writers := make([]io.Writer, ALL_SHAREDS)
	var err error
	for i := range writers {
		writers[i],err = objectstream.NewTempPutStream(servers[i], fmt.Sprintf(`%s.%d`,
			hash, i), perShared)
		if err != nil {
			return nil, err
		}
	}

	enc := NewEncoder(writers)
	return &RSPutStream{enc},nil
}


type encoder struct {
	writers []io.Writer
	enc reedsolomon.Encoder
	cache []byte
}

func NewEncoder(writes []io.Writer) *encoder {
	enc,_ := reedsolomon.New(DATA_SHAREDS, PARITY_SHAREDS)
	return &encoder{writes, enc, nil}
}

func (e *encoder) Write(p []byte) (n int, err error) {
	current, length := 0, len(p)
	for length > 0 {
		next := BLOCK_PRE_SHARED - len(e.cache)
		if next > length {
			next = length
		}

		e.cache = append(e.cache, p[current:current + next]...)
		if len(e.cache) == BLOCK_SIZE {
			e.Flush()
		}

		current += next
		length -= next
	}

	return len(p),nil
}

func (e *encoder) Flush() {
	if len(e.cache) < 1 {
		return
	}

	shared,_ := e.enc.Split(e.cache)
	e.enc.Encode(shared)
	for i := range shared {
		e.writers[i].Write(shared[i])
	}

	e.cache = []byte{}
}

func (s *RSPutStream)Commit(ok bool) {
	s.Flush()
	for i := range s.writers {
		s.writers[i].(*objectstream.TempPutStream).Commit(ok)
	}
}

type RSGetStream struct {
	*decoder
}

func NewRSGetStream(locateInfo map[int]string,
	dataServer[]string, hash string, size int64) (*RSGetStream, error){
	if len(locateInfo) + len(dataServer) != ALL_SHAREDS {
		return nil, fmt.Errorf(`dataServer number mismatch`)
	}

	readers := make([]io.Reader, ALL_SHAREDS)
	for i := 0; i < ALL_SHAREDS;i++ {
		server := locateInfo[i]
		if server != "" {
			locateInfo[i] = dataServer[0]
			dataServer = dataServer[1:]
			continue
		}

		reader,err := objectstream.NewGetStream(server, fmt.Sprintf(`%s.%d`, hash, i))
		if err == nil {
			readers[i] = reader
		}
	}


	writers := make([]io.Writer, ALL_SHAREDS)
	perShared := (size + DATA_SHAREDS - 1) / DATA_SHAREDS
	var err error
	for i := range readers {
		if readers[i] == nil {
			writers[i],err = objectstream.NewTempPutStream(locateInfo[i], fmt.Sprintf(`%s.%d`, hash, i), perShared)
			if err != nil {
				return nil, err
			}
		}
	}

	dec := NewDecoder(readers, writers, size)
	return &RSGetStream{dec}, nil
}

type decoder struct {
	readers []io.Reader
	writers []io.Writer
	enc reedsolomon.Encoder
	size int64
	cache []byte
	cacheSize int
	total int64
}

func NewDecoder(readres []io.Reader, writers []io.Writer, size int64) *decoder {
	enc,_ := reedsolomon.New(DATA_SHAREDS, PARITY_SHAREDS)
	return &decoder{readers: readres, writers: writers, enc:enc, size: size, cache: nil, cacheSize: 0, total: 0}
}

func (d *decoder) Read(p []byte)(n int, err error) {
	if d.cacheSize == 0 {
		err = d.getData()
		if err != nil {
			return 0,err
		}
	}

	length := len(p)
	if d.cacheSize < length {
		length = d.cacheSize
	}

	d.cacheSize -= length
	copy(p, d.cache[:length])
	d.cache = d.cache[length:]
	return length, nil
}

func (d *decoder) getData() error {
	if d.total == d.size {
		return io.EOF
	}

	shareds := make([][]byte, ALL_SHAREDS)
	repairIds := make([]int, 0)
	for i := range shareds {
		if d.readers[i] == nil {
			repairIds = append(repairIds, i)
		} else {
			shareds[i] = make([]byte, BLOCK_PRE_SHARED)
			n,e := io.ReadFull(d.readers[i], shareds[i])
			if e != nil && e != io.EOF && e != io.ErrUnexpectedEOF {
				shareds[i] = nil
			} else if n != BLOCK_PRE_SHARED {
				shareds[i] = shareds[i][:n]
			}
		}
	}

	e := d.enc.Reconstruct(shareds)
	if e != nil {
		return e
	}

	for i := range repairIds {
		id := repairIds[i]
		d.writers[id].Write(shareds[id])
	}

	for i := 0; i < DATA_SHAREDS; i++ {
		sharedSize := int64(len(shareds[i]))
		if d.total + sharedSize > d.size {
			sharedSize = d.total + sharedSize - d.size
		}

		d.cache = append(d.cache, shareds[i][:sharedSize]...)
		d.cacheSize += int(sharedSize)
		d.total += sharedSize
	}

	return nil
}

func(s *RSGetStream) Close() error {
	for i := range s.writers {
		if s.writers[i] != nil {
			s.writers[i].(*objectstream.TempPutStream).Commit(true)
		}
	}

	return nil
}



