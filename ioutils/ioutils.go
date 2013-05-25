package ioutils

import (
	"bufio"
	"io"
	"os"
)

func FileReader(path string) io.Reader {
	file, err := os.Open(path)
	if err != nil {
		panic("File not found : " + path)
		return nil
	}
	return bufio.NewReader(file)
}
