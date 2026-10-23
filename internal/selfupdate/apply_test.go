package selfupdate

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"intercept/internal/infrastructure/secrets"
)

func TestActivateApplicationStartsValidatedProcess(t *testing.T) {
	baseDir := t.TempDir()
	tokenPath := filepath.Join(baseDir, "data", "access.dat")
	const token = "test-access-token-with-at-least-32-characters"
	if err := secrets.Save(tokenPath, []byte(token)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/control/start" {
			t.Fatalf("unexpected activation request %s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("X-EAVC-Access") != token {
			t.Fatal("activation request has no protected access token")
		}
		if request.Header.Get("Origin") != "http://"+request.Host {
			t.Fatalf("unexpected origin %q", request.Header.Get("Origin"))
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"version": "0.2.0-rc46", "state": "running", "configured": true, "processing": true,
		})
	}))
	defer server.Close()
	options := ApplyOptions{
		BaseDir: baseDir, TokenFile: filepath.ToSlash(filepath.Join("data", "access.dat")),
		Address: strings.TrimPrefix(server.URL, "http://"),
	}
	if err := activateApplication(options, "0.2.0-rc46"); err != nil {
		t.Fatalf("activate validated application: %v", err)
	}
}

func TestActivateApplicationRejectsPausedConfiguredProcess(t *testing.T) {
	baseDir := t.TempDir()
	tokenPath := filepath.Join(baseDir, "access.dat")
	if err := secrets.Save(tokenPath, []byte("test-access-token-with-at-least-32-characters")); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"version": "0.2.0-rc46", "state": "paused", "configured": true, "processing": false,
		})
	}))
	defer server.Close()
	err := activateApplication(ApplyOptions{
		BaseDir: baseDir, TokenFile: "access.dat", Address: strings.TrimPrefix(server.URL, "http://"),
	}, "0.2.0-rc46")
	if err == nil || !strings.Contains(err.Error(), "обработка не запущена") {
		t.Fatalf("paused configured application must fail activation: %v", err)
	}
}
