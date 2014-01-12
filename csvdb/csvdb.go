package csvdb

import (
	"encoding/csv"
	"github.com/aniljava/goutils/generalutils"
	"github.com/aniljava/goutils/ioutils"
	"os"
)

type CSVDB struct {
	Header    []string
	file      *os.File
	writer    *csv.Writer
	data      [][]string
	writemode bool
	index     int
}

func example() {
	db := NewWithHeader("/root/Desktop/one.csv", []string{"id", "Apple"})
	db.Close()
}

func OpenWithHeader(name string) *CSVDB {
	file := ioutils.OpenFile(name)
	defer file.Close()
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	if data, err := reader.ReadAll(); err == nil {
		db := CSVDB{
			Header:    data[0],
			index:     1,
			data:      data,
			writemode: false,
		}
		return &db
	} else {
		panic(err)
	}
	return nil

}

func NewWithHeader(name string, headers []string) *CSVDB {
	file := ioutils.CreateFile(name)
	db := CSVDB{
		Header:    headers,
		file:      file,
		writer:    csv.NewWriter(file),
		writemode: true,
	}
	db.Write(headers)
	return &db
}

func (writer *CSVDB) Write(record []string) {
	writer.writer.Write(record)
	writer.writer.Flush()
}
func (writer *CSVDB) Close() {
	writer.writer.Flush()
	writer.file.Close()
}

func (writer *CSVDB) Next() []string {
	result := writer.data[writer.index]
	writer.index = writer.index + 1
	return result
}

func (writer *CSVDB) HasNext() bool {
	return len(writer.data) > writer.index
}

func (writer *CSVDB) Get(col string) string {
	index := generalutils.ArrayIndex(writer.Header, col)
	return writer.data[writer.index][index]
}

func (writer *CSVDB) FindRow(col string, val string) []string {
	for _, row := range writer.data {
		index := generalutils.ArrayIndex(writer.Header, col)
		if index < len(row) && val == row[index] {
			return row
		}
	}
	return nil
}

func (writer *CSVDB) FindCell(searchcol string, searchval string, col string) string {
	row := writer.FindRow(searchcol, searchval)

	index := generalutils.ArrayIndex(writer.Header, col)
	if index < len(row) {
		return row[index]
	}
	return ""
}
