// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package client

import (
	"io"

	"go.etcd.io/etcd/clientv3"
)

// ClusterCloser adds io.Closer to the clientv3.Cluster interface to enable closing the underlying resources.
type ClusterCloser interface {
	clientv3.Cluster
	io.Closer
}

// KVCloser adds io.Closer to the clientv3.KV interface to enable closing the underlying resources.
type KVCloser interface {
	clientv3.KV
	io.Closer
}

// MaintenanceCloser adds io.Closer to the clientv3.Maintenance interface to enable closing the underlying resources.
type MaintenanceCloser interface {
	clientv3.Maintenance
	io.Closer
}

// Factory interface defines a way to construct and close the client objects for different ETCD API.
type Factory interface {
	NewCluster() (ClusterCloser, error)
	NewKV() (KVCloser, error)
	NewMaintenance() (MaintenanceCloser, error)
	NewWatcher() (clientv3.Watcher, error) // clientv3.Watcher already supports io.Closer
}
