package etcdaccess

import (
	"context"
	"crypto/tls"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/transport"
)

// NewFactory returns a Factory that constructs new clients using the supplied ETCD client configuration.
func NewFactory(cfg brtypes.EtcdConnectionConfig, opts ...Option) Factory {
	options := &Options{}
	for _, opt := range opts {
		opt.ApplyTo(options)
	}

	var f = factoryImpl{
		EtcdConnectionConfig: cfg,
		options:              options,
	}

	return &f
}

// factoryImpl implements the client.Factory interface by constructing new client objects.
type factoryImpl struct {
	brtypes.EtcdConnectionConfig
	options *Options
}

func (f *factoryImpl) NewClient() (*clientv3.Client, error) {
	return getTLSClientForEtcd(&f.EtcdConnectionConfig, f.options)
}

func (f *factoryImpl) NewCluster() (ClusterCloser, error) {
	return f.NewClient()
}

func (f *factoryImpl) NewKV() (KVCloser, error) {
	return f.NewClient()
}

func (f *factoryImpl) NewMaintenance() (MaintenanceCloser, error) {
	return f.NewClient()
}

func (f *factoryImpl) NewWatcher() (clientv3.Watcher, error) {
	return f.NewClient()
}

// NewClientFactory returns the Factory using the supplied EtcdConnectionConfig.
func NewClientFactory(fn brtypes.NewClientFactoryFunc, cfg brtypes.EtcdConnectionConfig) Factory {
	if fn == nil {
		fn = NewFactory
	}
	return fn(cfg)
}

// getTLSClientForEtcd creates an etcd client using the TLS config params.
func getTLSClientForEtcd(tlsConfig *brtypes.EtcdConnectionConfig, options *Options) (*clientv3.Client, error) {
	// set tls if any one tls option set
	var cfgtls *transport.TLSInfo
	tlsinfo := transport.TLSInfo{}
	if tlsConfig.CertFile != "" {
		tlsinfo.CertFile = tlsConfig.CertFile
		cfgtls = &tlsinfo
	}

	if tlsConfig.KeyFile != "" {
		tlsinfo.KeyFile = tlsConfig.KeyFile
		cfgtls = &tlsinfo
	}

	if tlsConfig.CaFile != "" {
		tlsinfo.TrustedCAFile = tlsConfig.CaFile
		cfgtls = &tlsinfo
	}

	endpoints := tlsConfig.Endpoints
	if options.UseServiceEndpoints && len(tlsConfig.ServiceEndpoints) > 0 {
		endpoints = tlsConfig.ServiceEndpoints
	}

	cfg := &clientv3.Config{
		Endpoints: endpoints,
		Context:   context.TODO(), // TODO: Use the context comming as parameter.
	}

	if cfgtls != nil {
		clientTLS, err := cfgtls.ClientConfig()
		if err != nil {
			return nil, err
		}
		cfg.TLS = clientTLS
	}

	// if key/cert is not given but user wants secure connection, we
	// should still setup an empty tls configuration for gRPC to setup
	// secure connection.
	if cfg.TLS == nil && !tlsConfig.InsecureTransport {
		cfg.TLS = &tls.Config{}
	}

	// If the user wants to skip TLS verification then we should set
	// the InsecureSkipVerify flag in tls configuration.
	if tlsConfig.InsecureSkipVerify && cfg.TLS != nil {
		cfg.TLS.InsecureSkipVerify = true
	}

	if tlsConfig.Username != "" && tlsConfig.Password != "" {
		cfg.Username = tlsConfig.Username
		cfg.Password = tlsConfig.Password
	}

	return clientv3.New(*cfg)
}
