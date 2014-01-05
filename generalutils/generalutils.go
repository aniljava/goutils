package generalutils

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func ArrayIndex(array []string, key string) int {
	for i, v := range array {
		if v == key {
			return i
		}
	}
	return -1
}

func GetId(str string) string {
	//a-z|A-Z|0-9|-

	str = CompactTrim(str)
	str = strings.ToLower(str)
	result := ""

	arr := []byte(str)
	for _, b := range arr {
		if (b >= 48 && b <= 57) || (b >= 65 && b <= 90) || (b >= 97 && b <= 124) {
			result += string(b)
		} else {
			result += "-"
		}
	}
	return result
}

func ToString(iface interface{}) string {
	return fmt.Sprint(iface)
}

func Concat(ifaces ...interface{}) string {
	result := ""
	for _, iface := range ifaces {
		result += fmt.Sprint(iface)
	}
	return result
}

// Removes leading and following white spaces, convert new lines, tabs to
// space. Converts all double spaces to single space.
func CompactTrim(str string) string {
	str = replaceMultiple(str, " ", "\n", "\r", "\t")
	str = strings.TrimSpace(str)

	for strings.Index(str, "  ") != -1 {
		str = strings.Replace(str, "  ", " ", -1)
	}
	str = strings.TrimSpace(str)
	return str
}
func replaceMultiple(str, replace string, find ...string) string {
	for _, f := range find {
		str = strings.Replace(str, f, replace, -1)
	}
	return str

}

func ToInt(str string) int {
	if i, err := strconv.ParseInt(str, 10, 32); err == nil {
		return int(i)
	} else {
		panic(err)
	}
}

func ToFloat(str string) float64 {
	if i, err := strconv.ParseFloat(str, 64); err == nil {
		return i
	} else {
		panic(err)
	}
}

func NormalizeNumberString(str string) string {
	result := ""
	for _, b := range []byte(str) {
		if (b >= 48 && b <= 57) || b == 46 {
			result += string(b)
		}
	}
	return result
}

type Sortable struct {
	LenFx  func() int
	SwapFx func(i, j int)
	LessFx func(i, j int) bool
}

func (a Sortable) Len() int {
	return a.LenFx()
}
func (a Sortable) Swap(i, j int) {
	a.SwapFx(i, j)
}
func (a Sortable) Less(i, j int) bool {
	return a.LessFx(i, j)
}

func GetCurrentDirectory() string {
	_curr_dir, _ := filepath.Abs(".")
	_fs, _ := os.Stat(_curr_dir)
	return _fs.Name()
}

func ToTitleCase(str string) string {
	words := strings.Split(str, " ")
	result := ""

	for _, word := range words {
		if len(word) > 4 {
			word = strings.ToUpper(word[0:1]) + word[1:]
		}

		if result == "" {
			result = word
		} else {
			result += " " + word
		}
	}

	return result
}

func CSVEncoded(data ...string) string {
	w := bytes.Buffer{}

	writer := csv.NewWriter(&w)
	writer.Write(data)
	writer.Flush()
	line := w.String()
	return line[:len(line)-1]
}

func MapToArrayWithHeader(data map[string]string, header []string) []string {
	result := []string{}
	for _, k := range header {
		v := data[k]
		result = append(result, v)
	}
	return result
}
