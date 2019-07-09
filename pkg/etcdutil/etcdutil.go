// Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdutil

import (
	"crypto/tls"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
)

// NewTLSConfig returns the TLSConfig object.
func NewTLSConfig(cert, key, caCert string, insecureTr, skipVerify bool, endpoints []string, username, password string) *TLSConfig {
	return &TLSConfig{
		cert:       cert,
		key:        key,
		caCert:     caCert,
		insecureTr: insecureTr,
		skipVerify: skipVerify,
		endpoints:  endpoints,
		username:   username,
		password:   password,
	}
}

// GetTLSClientForEtcd creates an etcd client using the TLS config params.
func GetTLSClientForEtcd(tlsConfig *TLSConfig) (*clientv3.Client, error) {
	// set tls if any one tls option set
	var cfgtls *transport.TLSInfo
	tlsinfo := transport.TLSInfo{}
	if tlsConfig.cert != "" {
		tlsinfo.CertFile = tlsConfig.cert
		cfgtls = &tlsinfo
	}

	if tlsConfig.key != "" {
		tlsinfo.KeyFile = tlsConfig.key
		cfgtls = &tlsinfo
	}

	if tlsConfig.caCert != "" {
		tlsinfo.CAFile = tlsConfig.caCert
		cfgtls = &tlsinfo
	}

	cfg := &clientv3.Config{
		Endpoints: tlsConfig.endpoints,
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
	if cfg.TLS == nil && !tlsConfig.insecureTr {
		cfg.TLS = &tls.Config{}
	}

	// If the user wants to skip TLS verification then we should set
	// the InsecureSkipVerify flag in tls configuration.
	if tlsConfig.skipVerify && cfg.TLS != nil {
		cfg.TLS.InsecureSkipVerify = true
	}

	if tlsConfig.username != "" && tlsConfig.password != "" {
		cfg.Username = tlsConfig.username
		cfg.Password = tlsConfig.password
	}

	return clientv3.New(*cfg)
}
