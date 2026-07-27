package api

import (
	"bytes"
	"io"
	"net/http"
	"time"
)

const (
	defaultHTTPReadHeaderTimeout = 10 * time.Second
	defaultHTTPReadTimeout       = 60 * time.Second
	defaultHTTPWriteTimeout      = 120 * time.Second
	defaultHTTPIdleTimeout       = 120 * time.Second
	defaultHTTPMaxHeaderBytes    = 1 << 20
	defaultHTTPMaxBodyBytes      = 64 << 20
)

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           limitRequestBody(handler, defaultHTTPMaxBodyBytes),
		ReadHeaderTimeout: defaultHTTPReadHeaderTimeout,
		ReadTimeout:       defaultHTTPReadTimeout,
		WriteTimeout:      defaultHTTPWriteTimeout,
		IdleTimeout:       defaultHTTPIdleTimeout,
		MaxHeaderBytes:    defaultHTTPMaxHeaderBytes,
	}
}

func limitRequestBody(next http.Handler, maxBytes int64) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Body == nil || request.Body == http.NoBody {
			next.ServeHTTP(response, request)
			return
		}
		if request.ContentLength > maxBytes {
			writeRequestTooLarge(response)
			return
		}

		if request.ContentLength < 0 {
			body, err := io.ReadAll(io.LimitReader(request.Body, maxBytes+1))
			_ = request.Body.Close()
			if err != nil {
				http.Error(response, "invalid request body", http.StatusBadRequest)
				return
			}
			if int64(len(body)) > maxBytes {
				writeRequestTooLarge(response)
				return
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			request.ContentLength = int64(len(body))
		} else {
			request.Body = http.MaxBytesReader(response, request.Body, maxBytes)
		}

		next.ServeHTTP(response, request)
	})
}

func writeRequestTooLarge(response http.ResponseWriter) {
	response.Header().Set("Content-Type", "application/json; charset=utf-8")
	response.WriteHeader(http.StatusRequestEntityTooLarge)
	_, _ = io.WriteString(response, `{"status":413,"msg":"request body too large"}`)
}
