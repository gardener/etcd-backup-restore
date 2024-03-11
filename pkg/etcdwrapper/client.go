package etcdwrapper

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gardener/etcd-backup-restore/pkg/types"
)

type Client struct {
	*http.Client
	baseAddress string
}

func NewClient(e *types.EtcdConnectionConfig) (*Client, error) {
	tlsEnabled := isTLSEnabled(e)
	client, err := createClient(tlsEnabled, e.EtcdWrapperHostPort, e.CaFile)
	if err != nil {
		return nil, err
	}

	return &Client{
		Client:      client,
		baseAddress: constructBaseAddress(tlsEnabled, e.EtcdWrapperHostPort),
	}, nil
}

func (c *Client) createAndExecuteHTTPRequest(ctx context.Context, method, url string) (*http.Response, error) {
	// create cancellable child context for http request
	httpCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// create new request
	req, err := http.NewRequestWithContext(httpCtx, method, url, nil)
	if err != nil {
		return nil, err
	}

	// send http request
	response, err := c.Do(req)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (c *Client) StopWrapperProcess(ctx context.Context) error {
	url := fmt.Sprintf("%s/stop", c.baseAddress)
	response, err := c.createAndExecuteHTTPRequest(ctx, http.MethodPost, url)
	if err != nil {
		return err
	}
	defer closeResponseBody(response)

	if !responseHasOKCode(response) {
		return fmt.Errorf("server returned error response code when attempting to trigger stop on the etcd-wrapper server: %v", response)
	}
	return nil
}
