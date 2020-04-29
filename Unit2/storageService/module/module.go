package module

import (
	"io"
	"log"
	"os"
)

func StorageObject(fullPath string, r io.Reader) error {
	// os.Create:如果文件存在则清空内容,底层调用OpenFile(name, O_RDWR|O_CREATE|O_TRUNC, 0666)
	file,err := os.Create(fullPath)
	if err != nil {
		log.Print(err)
		return err
	}
	defer file.Close()

	io.Copy(file, r)

	return nil
}

func LoadObject(fullPath string, w io.Writer) error {
	file,err := os.Open(fullPath)
	if err != nil {
		log.Print(err)
		return err
	}
	defer file.Close()

	io.Copy(w, file)

	return nil
}
