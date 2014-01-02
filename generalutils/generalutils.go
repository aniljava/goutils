package generalutils

import (
	"fmt"
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
