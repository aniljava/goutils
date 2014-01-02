package ioutils

import (
	"os"
)

func CreateFile(path string) *os.File {
	file, err := os.Create(path)
	if err != nil {
		panic(err.Error())
	}
	return file
}

func OpenFile(path string) *os.File {
	file, err := os.Open(path)
	if err != nil {
		panic(err.Error())
	}
	return file
}
