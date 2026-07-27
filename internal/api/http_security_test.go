package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewHTTPServerHasSecurityLimits(t *testing.T) {
	server := newHTTPServer("127.0.0.1:0", http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))

	if server.ReadHeaderTimeout != defaultHTTPReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout = %s, want %s", server.ReadHeaderTimeout, defaultHTTPReadHeaderTimeout)
	}
	if server.ReadTimeout != defaultHTTPReadTimeout {
		t.Fatalf("ReadTimeout = %s, want %s", server.ReadTimeout, defaultHTTPReadTimeout)
	}
	if server.WriteTimeout != defaultHTTPWriteTimeout {
		t.Fatalf("WriteTimeout = %s, want %s", server.WriteTimeout, defaultHTTPWriteTimeout)
	}
	if server.IdleTimeout != defaultHTTPIdleTimeout {
		t.Fatalf("IdleTimeout = %s, want %s", server.IdleTimeout, defaultHTTPIdleTimeout)
	}
	if server.MaxHeaderBytes != defaultHTTPMaxHeaderBytes {
		t.Fatalf("MaxHeaderBytes = %d, want %d", server.MaxHeaderBytes, defaultHTTPMaxHeaderBytes)
	}
}

func TestLimitRequestBodyRejectsOversizedKnownLength(t *testing.T) {
	called := false
	handler := limitRequestBody(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}), 4)
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("12345"))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if called {
		t.Fatal("oversized request reached the application handler")
	}
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestLimitRequestBodyRejectsOversizedChunkedBody(t *testing.T) {
	called := false
	handler := limitRequestBody(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	}), 4)
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("12345"))
	request.ContentLength = -1
	request.TransferEncoding = []string{"chunked"}
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if called {
		t.Fatal("oversized chunked request reached the application handler")
	}
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestLimitRequestBodyPreservesAllowedBody(t *testing.T) {
	handler := limitRequestBody(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Fatalf("read limited body: %v", err)
		}
		_, _ = response.Write(body)
	}), 4)
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("1234"))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	if response.Body.String() != "1234" {
		t.Fatalf("body = %q, want %q", response.Body.String(), "1234")
	}
}
