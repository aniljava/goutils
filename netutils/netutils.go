package netutils

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"mime/multipart"
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

func Upload(url string, data []byte) (body string, err error) {

	buffer := bytes.Buffer{}
	mp := multipart.NewWriter(&buffer)

	fieldm, err := mp.CreateFormFile("data", "data")
	fieldm.Write(data)

	mp.Close()

	req, err := http.NewRequest("POST", url, &buffer)
	if err != nil {
		return
	}
	// Don't forget to set the content type, this will contain the boundary.
	req.Header.Set("Content-Type", mp.FormDataContentType())

	fmt.Print(req)

	if resp, err := client.Do(req); err == nil {
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
