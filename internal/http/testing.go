package http

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestServer create http test server
func TestServer(t *testing.T, code int, response string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
		if code == http.StatusOK {
			if response != "" {
				_, err := fmt.Fprint(rw, response)
				assert.NoError(t, err)
			} else {
				rw.WriteHeader(code)
			}
		} else {
			rw.WriteHeader(code)
		}
	}))
}

// TestFileServer create http test server
func TestFileServer(_ *testing.T, dir string) *httptest.Server {
	return httptest.NewServer(http.FileServer(http.Dir(dir)))
}
