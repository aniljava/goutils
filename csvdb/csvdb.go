package csvdb

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"github.com/aniljava/goutils/generalutils"
	"github.com/aniljava/goutils/ioutils"
	"github.com/gwenn/gosqlite"
	"io"
	"os"
	"strings"
)

type CSVDB struct {
	Header        []string
	file          *os.File
	writer        *csv.Writer
	data          [][]string
	writemode     bool
	index         int
	invertedIndex map[string]([]int)
	SQL           sqlite.Conn
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

func (db *CSVDB) SearchRows(key, val string) [][]string {
	result := [][]string{}

	if r, exists := db.invertedIndex[key+"-"+val]; exists {
		for _, i := range r {
			result = append(result, db.data[i])
		}
	}
	return result
}

func (db *CSVDB) SearchCells(key, val, col string) []string {
	index := generalutils.ArrayIndex(db.Header, col)
	result := []string{}
	if index == -1 {
		return result
	}
	search := db.SearchRows(key, val)

	for _, v := range search {
		result = append(result, v[index])
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

func (db *DB) Insert(data []string) {
	qs := strings.Repeat("?,", len(data))
	qs = qs[:len(qs)-1] // remove last coma
	if err := db.Conn.Exec("INSERT INTO CSV VALUES("+qs+")", generalutils.StrArrayToInterfaceArray(data)...); err != nil {
		panic(err)
	}
}

func (db *DB) QueryString(col string, clause string, args ...string) string {

	stmt, _ := db.Conn.Prepare("SELECT " + col + " FROM CSV WHERE " + clause)
	defer stmt.Finalize()
	stmt.Bind(args)
	if exists, _ := stmt.Next(); exists {
		val := make([]interface{}, 1)
		stmt.ScanValues(val)
		return val[0].(string)
	} else {
		return ""
	}

}

func (db *DB) QueryStringByKey(col string, key string, keyval string) string {
	return db.QueryString(col, "WHERE "+key+"=?", keyval)
}

type DB struct {
	FileName string
	Conn     *sqlite.Conn
	Header   []string
}

func (db *DB) SetHeader(header ...string) *DB {
	db.Header = header
	setHeader(db.Conn, header)
	return db
}

func Open(name string) *DB {
	db := DB{}
	db.FileName = name

	if ioutils.Exists(name) {
		if strings.HasSuffix(name, ".csv") {
			if conn, err := import_csv(name); err == nil {
				db.Conn = conn
			} else {
				return nil
			}
		} else {
			var err error
			if db.Conn, err = sqlite.Open(name); err != nil {
				panic(err)
			}
		}
	} else {
		if conn, err := sqlite.Open(":memory:"); err == nil {
			db.Conn = conn
		} else {
			return nil
		}
	}
	return &db
}

func setHeader(conn *sqlite.Conn, header []string) map[string]string {

	headermap := map[string]string{}

	if err := conn.Exec("DROP TABLE IF EXISTS CSV"); err != nil {
		panic(err)
	}
	conn.Exec("CREATE TABLE headermeta (id TEXT, name TEXT)")

	sql := ""
	for _, h := range header {
		id := toid(h)

		headermap[id] = h

		conn.Exec("INSERT INTO headermeta (?,?)", id, h)
		if sql == "" {
			sql = "CREATE TABLE IF NOT EXISTS CSV (" + id + " TEXT "
		} else {
			sql += ", " + id + " TEXT "
		}
	}
	sql += ")"

	if err := conn.Exec(sql); err != nil {
		panic(err)
	}
	return headermap
}

func toid(str string) string {
	id := generalutils.GetId(str)
	id = strings.Replace(id, "-", "_", -1)
	return id
}

func import_csv(name string) (*sqlite.Conn, error) {

	file := ioutils.OpenFile(name)
	defer file.Close()
	reader := csv.NewReader(file)
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.TrailingComma = true
	if data, err := reader.ReadAll(); err == nil {
		if db, err := sqlite.Open(":memory:"); err == nil {

			setHeader(db, data[0])
			qs := strings.Repeat("?,", len(data[0]))
			qs = qs[:len(qs)-1] // remove last coma
			if stmt, err := db.Prepare("INSERT INTO CSV VALUES(" + qs + ")"); err == nil {
				defer stmt.Finalize()
				db.Begin()
				for i, values := range data {
					if i != 0 {
						stmt.Exec(generalutils.StrArrayToInterfaceArray(values)...)
					}
				}
				db.Commit()
				return db, nil
			} else {
				return nil, err
			}

		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}
func (db *DB) CSVExport(writer io.Writer) error {

	header := db.Header

	if header == nil {
		cols, _ := db.Conn.Columns("main", "CSV")
		header := []string{}
		for _, table := range cols {
			header = append(header, table.Name)
		}
	}

	csv := csv.NewWriter(writer)
	csv.Write(header)
	csv.Flush()

	stmt, _ := db.Conn.Prepare("SELECT * FROM CSV ORDER BY " + header[0])
	defer stmt.Finalize()

	exists, err := stmt.Next()
	for exists && err == nil {
		val := make([]interface{}, len(header))
		stmt.ScanValues(val)
		csv.Write(generalutils.InterfaceArrayToStrArray(val))
		csv.Flush()
		exists, err = stmt.Next()
	}
	if err != nil {
		return err
	} else {
		return nil
	}
}
func (db *DB) CSVToBytes() []byte {
	w := bytes.NewBuffer(nil)
	db.CSVExport(w)
	return w.Bytes()
}
