package main

import (
	"os"
	"strings"
	"testing"
)

func TestDockerfilesIncludeLocalWkrpcBeforeModuleDownload(t *testing.T) {
	for _, name := range []string{"Dockerfile", "Dockerfile.arm64"} {
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			text := string(data)
			localModule := strings.Index(text, "ADD third_party/wkrpc ./third_party/wkrpc")
			download := strings.Index(text, "RUN go mod download")
			if localModule < 0 || download < 0 || localModule > download {
				t.Fatalf("%s must add the local wkrpc replacement before go mod download", name)
			}
		})
	}
}
