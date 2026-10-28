package selfupdate

import (
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"intercept/internal/infrastructure/secrets"
)

func TestActivationDialFailureAllowsRollback(t *testing.T) {
	err := &url.Error{Op: "Post", URL: "http://127.0.0.1:1/api/control/start", Err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}}
	if !activationRequestDefinitelyNotSent(err) {
		t.Fatal("отказ до TCP-соединения должен разрешать автоматический откат")
	}
	if activationRequestDefinitelyNotSent(&url.Error{Op: "Post", Err: errors.New("response body interrupted")}) {
		t.Fatal("потерю ответа после отправки нельзя считать безусловно безопасной для отката")
	}
}

func TestApplyPathsAreConfinedToManagedTempDirectory(t *testing.T) {
	workRoot, err := os.MkdirTemp("", "EAVC-Connect-Update-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(workRoot)
	valid := ApplyOptions{
		WorkRoot:  workRoot,
		Package:   filepath.Join(workRoot, "update.zip"),
		ReadyFile: filepath.Join(workRoot, "helper-ready"),
	}
	if err := validateApplyPaths(valid); err != nil {
		t.Fatalf("штатные пути Manager отклонены: %v", err)
	}
	malicious := valid
	malicious.Package = t.TempDir()
	if err := validateApplyPaths(malicious); err == nil {
		t.Fatal("произвольный путь удаления не должен приниматься helper-ом")
	}
}

func TestActivateApplicationStartsValidatedProcess(t *testing.T) {
	baseDir := t.TempDir()
	tokenPath := filepath.Join(baseDir, "data", "access.dat")
	const token = "test-access-token-with-at-least-32-characters"
	if err := secrets.Save(tokenPath, []byte(token)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if serveUpdateInstanceProof(writer, request, token) {
			return
		}
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
	committed, err := activateApplication(options, "0.2.0-rc46")
	if err != nil || !committed {
		t.Fatalf("activate validated application: %v", err)
	}
}

func TestActivateApplicationRejectsPausedConfiguredProcess(t *testing.T) {
	baseDir := t.TempDir()
	tokenPath := filepath.Join(baseDir, "access.dat")
	const token = "test-access-token-with-at-least-32-characters"
	if err := secrets.Save(tokenPath, []byte(token)); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if serveUpdateInstanceProof(writer, request, token) {
			return
		}
		writer.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"version": "0.2.0-rc46", "state": "paused", "configured": true, "processing": false,
		})
	}))
	defer server.Close()
	committed, err := activateApplication(ApplyOptions{
		BaseDir: baseDir, TokenFile: "access.dat", Address: strings.TrimPrefix(server.URL, "http://"),
	}, "0.2.0-rc46")
	if err == nil || committed || !strings.Contains(err.Error(), "код 503") {
		t.Fatalf("paused configured application must fail activation: %v", err)
	}
}

func TestActivateApplicationWaitsUntilStartingProcessIsReady(t *testing.T) {
	baseDir := t.TempDir()
	tokenPath := filepath.Join(baseDir, "access.dat")
	const token = "test-access-token-with-at-least-32-characters"
	if err := secrets.Save(tokenPath, []byte(token)); err != nil {
		t.Fatal(err)
	}
	statusChecks := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if serveUpdateInstanceProof(writer, request, token) {
			return
		}
		if request.Method == http.MethodPost {
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"version": "0.2.0-rc50", "state": "starting", "configured": true, "processing": true,
			})
			return
		}
		statusChecks++
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"version": "0.2.0-rc50", "state": "running", "configured": true, "processing": true,
		})
	}))
	defer server.Close()
	committed, err := activateApplication(ApplyOptions{
		BaseDir: baseDir, TokenFile: "access.dat", Address: strings.TrimPrefix(server.URL, "http://"),
	}, "0.2.0-rc50")
	if err != nil || !committed {
		t.Fatalf("ожидание запуска завершилось ошибкой: %v", err)
	}
	if statusChecks == 0 {
		t.Fatal("обновлятор не дождался контрольного статуса после состояния starting")
	}
}

func TestHealthVersionDoesNotExposeTokenToUnverifiedPortOwner(t *testing.T) {
	baseDir := t.TempDir()
	tokenPath := filepath.Join(baseDir, "access.dat")
	const token = "secret-access-token-with-at-least-32-characters"
	if err := secrets.Save(tokenPath, []byte(token)); err != nil {
		t.Fatal(err)
	}
	var tokenObserved bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-EAVC-Access") != "" {
			tokenObserved = true
		}
		writer.Header().Set(updateInstanceProofHeader, "forged-proof")
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	_, err := healthVersion(ApplyOptions{
		BaseDir: baseDir, TokenFile: "access.dat", Address: strings.TrimPrefix(server.URL, "http://"),
	})
	if err == nil {
		t.Fatal("поддельный владелец порта ошибочно прошёл HMAC-проверку")
	}
	if tokenObserved {
		t.Fatal("bearer-токен раскрыт до подтверждения владельца порта")
	}
}

func serveUpdateInstanceProof(writer http.ResponseWriter, request *http.Request, token string) bool {
	if request.URL.Path != "/api/instance-proof" {
		return false
	}
	challenge := request.Header.Get(updateInstanceChallengeHeader)
	writer.Header().Set(updateInstanceProofHeader, updateInstanceProof(token, challenge))
	writer.WriteHeader(http.StatusNoContent)
	return true
}
