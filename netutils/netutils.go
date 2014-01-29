package netutils

import (
	"io/ioutil"
	"net/http"
)

func Get(url string) (body string, err error) {

	if resp, err := http.Get(url); err == nil {
		if content, err := ioutil.ReadAll(resp.Body); err == nil {
			body = string(content)
		}
	}
	return
}
