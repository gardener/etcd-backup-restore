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

package restorer

import (
	"net/url"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
)

const (
	tmpDir                  = "/tmp"
	tmpEventsDataFilePrefix = "etcd-restore-"

	defaultName                     = "default"
	defaultInitialAdvertisePeerURLs = "http://localhost:2380"
	defaultInitialClusterToken      = "etcd-cluster"
	defaultMaxFetchers              = 6
	defaultMaxCallSendMsgSize       = 10 * 1024 * 1024 //10Mib
	defaultMaxRequestBytes          = 10 * 1024 * 1024 //10Mib
	defaultMaxTxnOps                = 10 * 1024
	defaultEmbeddedEtcdQuotaBytes   = 8 * 1024 * 1024 * 1024 //8Gib
	defaultAutoCompactionMode       = "periodic"             // only 2 mode is supported: 'periodic' or 'revision'
	defaultAutoCompactionRetention  = "30m"
)

// Restorer is a struct for etcd data directory restorer
type Restorer struct {
	logger    *logrus.Entry
	zapLogger *zap.Logger
	store     snapstore.SnapStore
}

// RestoreOptions hold all snapshot restore related fields
// Note: Please ensure DeepCopy and DeepCopyInto are properly implemented.
type RestoreOptions struct {
	Config      *RestorationConfig
	ClusterURLs types.URLsMap
	PeerURLs    types.URLs
	// Base full snapshot + delta snapshots to restore from
	BaseSnapshot  snapstore.Snapshot
	DeltaSnapList snapstore.SnapList
}

// RestorationConfig holds the restoration configuration.
// Note: Please ensure DeepCopy and DeepCopyInto are properly implemented.
type RestorationConfig struct {
	InitialCluster           string   `json:"initialCluster"`
	InitialClusterToken      string   `json:"initialClusterToken,omitempty"`
	RestoreDataDir           string   `json:"restoreDataDir,omitempty"`
	InitialAdvertisePeerURLs []string `json:"initialAdvertisePeerURLs"`
	Name                     string   `json:"name"`
	SkipHashCheck            bool     `json:"skipHashCheck,omitempty"`
	MaxFetchers              uint     `json:"maxFetchers,omitempty"`
	MaxRequestBytes          uint     `json:"MaxRequestBytes,omitempty"`
	MaxTxnOps                uint     `json:"MaxTxnOps,omitempty"`
	MaxCallSendMsgSize       int      `json:"maxCallSendMsgSize,omitempty"`
	EmbeddedEtcdQuotaBytes   int64    `json:"embeddedEtcdQuotaBytes,omitempty"`
	AutoCompactionMode       string   `json:"autoCompactionMode,omitempty"`
	AutoCompactionRetention  string   `json:"autoCompactionRetention,omitempty"`
}

type initIndex int

func (i *initIndex) ConsistentIndex() uint64 {
	return uint64(*i)
}

// event is wrapper over etcd event to keep track of time of event
type event struct {
	EtcdEvent *clientv3.Event `json:"etcdEvent"`
	Time      time.Time       `json:"time"`
}

type fetcherInfo struct {
	Snapshot  snapstore.Snapshot
	SnapIndex int
}

type applierInfo struct {
	EventsFilePath string
	SnapIndex      int
}

// DeepCopyInto copies the structure deeply from in to out.
func (in *RestoreOptions) DeepCopyInto(out *RestoreOptions) {
	*out = *in
	if in.Config != nil {
		in, out := &in.Config, &out.Config
		*out = new(RestorationConfig)
		(*in).DeepCopyInto(*out)
	}
	if in.ClusterURLs != nil {
		in, out := &in.ClusterURLs, &out.ClusterURLs
		*out = make(types.URLsMap)
		for k := range *in {
			if (*in)[k] != nil {
				(*out)[k] = DeepCopyURLs((*in)[k])
			}
		}
	}
	if in.PeerURLs != nil {
		out.PeerURLs = DeepCopyURLs(in.PeerURLs)
	}
	if in.DeltaSnapList != nil {
		out.DeltaSnapList = DeepCopySnapList(in.DeltaSnapList)
	}
}

// DeepCopyURLs returns a deeply copy
func DeepCopyURLs(in types.URLs) types.URLs {
	out := make(types.URLs, len(in))
	for i, u := range in {
		out[i] = *(DeepCopyURL(&u))
	}
	return out
}

// DeepCopyURL returns a deeply copy
func DeepCopyURL(in *url.URL) *url.URL {
	var out = new(url.URL)
	*out = *in
	if in.User != nil {
		in, out := &in.User, &out.User
		*out = new(url.Userinfo)
		*out = *in
	}
	return out
}

// DeepCopySnapList returns a deep copy
func DeepCopySnapList(in snapstore.SnapList) snapstore.SnapList {
	out := make(snapstore.SnapList, len(in))
	for i, v := range in {
		if v != nil {
			var cpv = *v
			out[i] = &cpv
		}
	}
	return out
}

// DeepCopy returns a deeply copied structure.
func (in *RestoreOptions) DeepCopy() *RestoreOptions {
	if in == nil {
		return nil
	}

	out := new(RestoreOptions)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto copies the structure deeply from in to out.
func (in *RestorationConfig) DeepCopyInto(out *RestorationConfig) {
	*out = *in
	if in.InitialAdvertisePeerURLs != nil {
		in, out := &in.InitialAdvertisePeerURLs, &out.InitialAdvertisePeerURLs
		*out = make([]string, len(*in))
		for i, v := range *in {
			(*out)[i] = v
		}
	}
}

// DeepCopy returns a deeply copied structure.
func (in *RestorationConfig) DeepCopy() *RestorationConfig {
	if in == nil {
		return nil
	}

	out := new(RestorationConfig)
	in.DeepCopyInto(out)
	return out
}
