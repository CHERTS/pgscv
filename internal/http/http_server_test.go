package http

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAuthConfig_Validate(t *testing.T) {
	testcases := []struct {
		valid    bool
		cfg      AuthConfig
		wantAuth bool
		wantTLS  bool
	}{
		{valid: true, cfg: AuthConfig{}, wantAuth: false, wantTLS: false},
		{valid: true, cfg: AuthConfig{Username: "user", Password: "pass"}, wantAuth: true, wantTLS: false},
		{valid: true, cfg: AuthConfig{Keyfile: "key", Certfile: "cert"}, wantAuth: false, wantTLS: true},
		{valid: false, cfg: AuthConfig{Username: "user", Password: ""}},
		{valid: false, cfg: AuthConfig{Username: "", Password: "pass"}},
		{valid: false, cfg: AuthConfig{Keyfile: "key", Certfile: ""}},
		{valid: false, cfg: AuthConfig{Keyfile: "", Certfile: "cert"}},
	}

	for _, tc := range testcases {
		auth, tls, err := tc.cfg.Validate()
		if tc.valid {
			assert.NoError(t, err)
			assert.Equal(t, tc.wantAuth, auth)
			assert.Equal(t, tc.wantTLS, tls)
		} else {
			assert.Error(t, err)
		}
	}
}

func TestServer_Serve_HTTP(t *testing.T) {
	addr := "127.0.0.1:17890"
	srv := NewServer(ServerConfig{Addr: addr}, getDummyHandler(), getDummyHandler())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := srv.Serve()
		assert.NoError(t, err)
		wg.Done()
	}()

	time.Sleep(100 * time.Millisecond)

	cl := NewClient(ClientConfig{})
	endpoints := []string{"/", "/metrics"}

	for _, e := range endpoints {
		resp, err := cl.Get("http://" + addr + e)
		assert.NoError(t, err)
		assert.Equal(t, StatusOK, resp.StatusCode)
	}
}

func getDummyHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(http.ResponseWriter, *http.Request) {
	}
}

func TestServer_Serve_HTTPS(t *testing.T) {
	addr := "127.0.0.1:17891"
	srv := NewServer(ServerConfig{Addr: addr, AuthConfig: AuthConfig{
		EnableTLS: true,
		Keyfile:   "./testdata/example.key",
		Certfile:  "./testdata/example.crt",
	}}, getDummyHandler(), getDummyHandler())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := srv.Serve()
		assert.NoError(t, err)
		wg.Done()
	}()

	time.Sleep(100 * time.Millisecond)

	cl := NewClient(ClientConfig{})
	cl.EnableTLSInsecure()
	endpoints := []string{"/", "/metrics"}

	for _, e := range endpoints {
		resp, err := cl.Get("http://" + addr + e)
		assert.NoError(t, err)
		assert.NotEqual(t, StatusOK, resp.StatusCode)

		resp, err = cl.Get("https://" + addr + e)
		assert.NoError(t, err)
		assert.Equal(t, StatusOK, resp.StatusCode)
	}
}

func Test_handleRoot(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	res := httptest.NewRecorder()

	mux := http.NewServeMux()
	mux.Handle("/", handleRoot())
	mux.ServeHTTP(res, req)

	assert.Equal(t, StatusOK, res.Code)

	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	assert.Contains(t, string(body), `pgSCV / PostgreSQL metrics collector, for more info visit <a href="https://github.com/cherts/pgscv">Github</a> page.`)
	res.Flush()
}

func Test_basicAuth(t *testing.T) {
	testcases := []struct {
		name   string
		user   string
		pass   string
		status int
	}{
		{name: "valid", user: "user", pass: "pass", status: StatusOK},
		{name: "empty creds", user: "", pass: "", status: StatusUnauthorized},
		{name: "empty pass", user: "user", pass: "", status: StatusUnauthorized},
		{name: "empty user", user: "", pass: "pass", status: StatusUnauthorized},
		{name: "invalid pass", user: "user", pass: "invalid", status: StatusUnauthorized},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/", basicAuth(AuthConfig{Username: "user", Password: "pass"}, handleRoot()))

			res := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.SetBasicAuth(tc.user, tc.pass)
			mux.ServeHTTP(res, req)
			assert.Equal(t, tc.status, res.Code)
			res.Flush()
		})
	}
}
