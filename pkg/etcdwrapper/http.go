package etcdwrapper

import (
	"fmt"
	"github.com/gardener/etcd-backup-restore/pkg/types"
	"net/http"
	"strings"
	"time"
)

const (
	// schemeHTTP indicates a constant for the http scheme
	schemeHTTP = "http"
	// schemeHTTPS indicates a constant for the https scheme
	schemeHTTPS              = "https"
	httpClientRequestTimeout = 30 * time.Second
)

var (
	okResponseCodes = []int{http.StatusAccepted, http.StatusOK, http.StatusCreated}
)

// responseHasOKCode checks if the response is one of the accepted OK responses.
func responseHasOKCode(response *http.Response) bool {
	for _, code := range okResponseCodes {
		if code == response.StatusCode {
			return true
		}
	}
	return false
}

// closeResponseBody closes the response body if the response is not nil.
// As per https://pkg.go.dev/net/http - The http Client and Transport guarantee that Body is always
// non-nil, even on responses without a body or responses with a zero-length body. It is the caller's responsibility to
// close Body. The default HTTP client's Transport may not reuse HTTP/1.x "keep-alive" TCP connections if the Body is
// not read to completion and closed.
func closeResponseBody(response *http.Response) {
	if response != nil {
		_ = response.Body.Close()
	}
}

// constructBaseAddress creates a base address selecting a scheme based on tlsEnabled and using hostPort.
func constructBaseAddress(tlsEnabled bool, hostPort string) string {
	scheme := schemeHTTP
	if tlsEnabled {
		scheme = schemeHTTPS
	}
	return fmt.Sprintf("%s://%s", scheme, hostPort)
}

func isTLSEnabled(e *types.EtcdConnectionConfig) bool {
	return strings.TrimSpace(e.CertFile) != "" && strings.TrimSpace(e.KeyFile) != "" && strings.TrimSpace(e.CaFile) != ""
}

// getHost extracts the backup-restore server host from host-port string.
func getHost(hostPort string) string {
	host := "localhost"
	splits := strings.Split(hostPort, ":")
	if len(strings.TrimSpace(splits[0])) > 0 {
		host = splits[0]
	}
	return host
}

func createClient(tlsEnabled bool, hostPort string, caFilePath string) (*http.Client, error) {
	var transport *http.Transport

	if tlsEnabled {
		tlsConfig, err := createTLSConfig(getHost(hostPort), caFilePath, nil)
		if err != nil {
			return nil, err
		}
		transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   httpClientRequestTimeout,
	}
	return client, nil
}
