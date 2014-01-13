package csvdb

import (
	"encoding/csv"
	"fmt"
	"github.com/aniljava/goutils/generalutils"
	"github.com/aniljava/goutils/ioutils"
	"os"
)

type CSVDB struct {
	Header        []string
	file          *os.File
	writer        *csv.Writer
	data          [][]string
	writemode     bool
	index         int
	invertedIndex map[string]([]int)
}

func example() {
	db := NewWithHeader("/root/Desktop/one.csv", []string{"id", "Apple"})
	db.Close()
}

func OpenWithIndex(name string, indices ...string) *CSVDB {
	file := ioutils.OpenFile(name)
	defer file.Close()
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.TrailingComma = true
	if data, err := reader.ReadAll(); err == nil {
		fmt.Println(len(data))
		db := CSVDB{
			Header:        data[0],
			index:         1,
			data:          data,
			writemode:     false,
			invertedIndex: map[string]([]int){},
		}

		for i, row := range data {
			for _, h := range indices {
				k := generalutils.ArrayIndex(data[0], h)
				if k != -1 {
					val := row[k]
					if r, exists := db.invertedIndex[h+"-"+val]; exists {
						r = append(r, i)
					} else {
						db.invertedIndex[h+"-"+val] = []int{i}
					}
				}
			}
			fmt.Println(i)
		}

		return &db
	} else {
		panic(err)
	}
	return nil
}

func OpenWithHeader(name string) *CSVDB {
	file := ioutils.OpenFile(name)
	defer file.Close()
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.TrailingComma = true
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

func (db *CSVDB) Search(key, val string) [][]string {
	result := [][]string{}

	if r, exists := db.invertedIndex[key+"-"+val]; exists {
		for _, i := range r {
			result = append(result, db.data[i])
		}
	}
	return result
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

func (writer *CSVDB) Filter(col string, val string) *CSVDB {
	index := generalutils.ArrayIndex(writer.Header, col)
	if index == -1 {
		return nil
	}
	data := [][]string{}
	data = append(data, writer.Header)

	for _, row := range writer.data {
		if val == row[index] {
			data = append(data, row)
		}
	}
	db := CSVDB{
		Header: writer.Header,
		data:   data,
		index:  1,
	}
	return &db
}

func (writer *CSVDB) FindRow(col string, val string) []string {
	for _, row := range writer.data {
		index := generalutils.ArrayIndex(writer.Header, col)
		if index == -1 {
			return nil
		}
		if index < len(row) && val == row[index] {
			return row
		}
	}
	return nil
}

func (writer *CSVDB) FindCell(searchcol string, searchval string, col string) string {
	row := writer.FindRow(searchcol, searchval)

	index := generalutils.ArrayIndex(writer.Header, col)
	if index == -1 {
		fmt.Println(writer.Header, col)
		return ""
	}
	if index < len(row) {
		return row[index]
	}
	return ""
}
