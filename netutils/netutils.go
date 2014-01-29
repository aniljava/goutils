package netutils

import (
	"crypto/tls"
	"io/ioutil"
	"net/http"
	"strings"
)

var client *http.Client

func init() {
	config := &tls.Config{InsecureSkipVerify: true} // this line here
	tr := &http.Transport{TLSClientConfig: config}
	client = &http.Client{Transport: tr}
}

func Get(url string) (body string, err error) {

	if resp, err := client.Get(url); err == nil {
		if content, err := ioutil.ReadAll(resp.Body); err == nil {
			body = string(content)
			return body, nil
		} else {
			return "", err
		}
	} else {
		return "", err
	}
}

func Post(url string, data string) (body string, err error) {

	if resp, err := client.Post(url, "text/plain", strings.NewReader(data)); err == nil {
		if content, err := ioutil.ReadAll(resp.Body); err == nil {
			body = string(content)
			return body, nil
		} else {
			return "", err
		}
	} else {
		return "", err
	}
}
