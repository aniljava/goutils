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

func (db *DB) Insert(data []string) error {
	qs := strings.Repeat("?,", len(data))
	qs = qs[:len(qs)-1] // remove last coma
	if err := db.Conn.Exec("INSERT INTO CSV VALUES("+qs+")", generalutils.StrArrayToInterfaceArray(data)...); err != nil {
		return err
	}
	return nil
}

func (db *DB) QueryString(col string, clause string, args ...string) string {

	stmt, err := db.Conn.Prepare("SELECT " + col + " FROM CSV WHERE " + clause)
	if err != nil {
		panic(err)
	}
	defer stmt.Finalize()

	if args != nil {
		a := generalutils.StrArrayToInterfaceArray(args)
		stmt.Bind(a...)
	}
	if exists, _ := stmt.Next(); exists {
		val := make([]interface{}, 1)
		stmt.ScanValues(val)
		if val[0] != nil {
			return val[0].(string)
		} else {
			return ""
		}
	} else {
		return ""
	}

}

func (db *DB) QueryStringByKey(col string, key string, keyval string) string {
	return db.QueryString(col, key+"=?", keyval)
}

type DB struct {
	CSVFile   string
	DBFile    string
	Conn      *sqlite.Conn
	Header    []string
	HeaderMap map[string]string
	Key       string
}

func (db *DB) QueryToArray(sql string, args ...interface{}) []map[string]string {
	result := []map[string]string{}

	stmt, err := db.Conn.Prepare(sql)
	if err != nil {
		panic(err)
	}
	defer stmt.Finalize()
	stmt.Bind(args...)

	exists, err := stmt.Next()
	for exists && err == nil {
		val := make([]interface{}, stmt.ColumnCount())
		stmt.ScanValues(val)
		names := stmt.ColumnNames()
		stmt.ScanValues(val)

		r := map[string]string{}
		for i, h := range names {
			v := val[i]
			value := ""
			if v != nil {
				value = v.(string)
			}

			r[h] = value
		}

		result = append(result, r)
		exists, err = stmt.Next()
	}
	return result
}

func (db *DB) SetKey(key string) *DB {
	db.Key = key
	return db
}

func (db *DB) SetHeader(header ...string) *DB {

	if err := db.Conn.Exec("DROP TABLE IF EXISTS CSV"); err != nil {
		panic(err)
	}

	if err := db.Conn.Exec("DROP TABLE IF EXISTS headermeta"); err != nil {
		panic(err)
	}

	db.Conn.Exec("CREATE TABLE headermeta (id TEXT, name TEXT)")
	db.Header = header
	db.HeaderMap = map[string]string{}

	sql := ""
	for _, h := range header {
		id := toid(h)
		db.HeaderMap[id] = h

		db.Conn.Exec("INSERT INTO headermeta (?,?)", id, h)
		if sql == "" {
			sql = "CREATE TABLE IF NOT EXISTS CSV (" + id + " TEXT "
		} else {
			sql += ", " + id + " TEXT "
		}

		if db.Key == id {
			sql += " PRIMARY KEY "
		}
	}
	sql += ")"

	if err := db.Conn.Exec(sql); err != nil {
		panic(err)
	}
	return db
}

func (db *DB) Iterate(sql string, args []interface{}) chan map[string]string {
	result := make(chan map[string]string)

	iterator := func() {
		stmt, err := db.Conn.Prepare(sql)
		if err != nil {
			panic(err)
		}
		stmt.Bind(args)
		defer stmt.Finalize()

		exists, err := stmt.Next()
		for exists && err == nil {
			val := make([]interface{}, stmt.ColumnCount())
			stmt.ScanValues(val)
			names := stmt.ColumnNames()

			r := map[string]string{}
			for i, h := range names {
				v := val[i]
				value := ""
				if v != nil {
					value = v.(string)
				}

				r[h] = value
			}

			result <- r
			exists, err = stmt.Next()
		}
	}

	go iterator()
	return result
}
func (db *DB) Close() {
	//Make changes
	db.Conn.Close()
}

func Open(name string) *DB {
	db := DB{}

	if name == "" {
		db.DBFile = ":memory:"
	} else if strings.HasSuffix(name, "csv") {
		db.CSVFile = name
		db.DBFile = ":memory:"
	} else if strings.HasSuffix(name, "db") {
		db.DBFile = name
	}

	var err error
	if db.Conn, err = sqlite.Open(db.DBFile); err != nil {
		panic(err)
	}

	if ioutils.Exists(db.CSVFile) && db.CSVFile != "" {
		db.MergeCSV(db.CSVFile)
	}

	return &db
}

func (db *DB) MergeCSV(path string) {
	in := OpenWithHeader(path)
	if cols, err := db.Conn.Columns("main", "CSV"); err != nil || len(cols) == 0 {
		db.SetHeader(in.Header...)
	}

	for in.HasNext() {
		values := in.Next()
		db.Insert(values)
	}
}

func toid(str string) string {
	id := generalutils.GetId(str)
	id = strings.Replace(id, "-", "_", -1)
	return id
}

func (db *DB) CSVExport(writer io.Writer) error {

	header := db.Header

	if header == nil || len(header) == 0 {
		cols, _ := db.Conn.Columns("main", "CSV")
		header = []string{}
		for _, table := range cols {
			header = append(header, table.Name)
		}
	}

	fmt.Println(db.Conn.Columns("main", "CSV"))

	csv := csv.NewWriter(writer)
	csv.Write(header)
	csv.Flush()

	stmt, _ := db.Conn.Prepare("SELECT * FROM CSV ORDER BY " + toid(header[0]))
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
