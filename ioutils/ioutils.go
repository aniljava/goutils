package ioutils

import (
	"io/ioutil"
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

func ReadFile(path string) string {
	if data, err := ioutil.ReadFile(path); err == nil {
		return string(data)
	} else {
		panic(err)
	}
}

func ListFileNames(path string) []string {
	result := []string{}
	if dir, err := ioutil.ReadDir(path); err == nil {
		for _, fi := range dir {
			result = append(result, fi.Name())
		}
	} else {
		return nil
	}
	return result
}
