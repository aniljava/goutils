package ioutils

import (
	"os"
)

/**
 * Errorless file. Panic in case of error.
 */
func File(path string) *os.File {
	file, err := os.Create(path)
	if err != nil {
		panic(err.Error())
	}
	return file
}
