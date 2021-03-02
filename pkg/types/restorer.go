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

package types

import (
	"fmt"
	"net/url"
	"path"
	"time"

	flag "github.com/spf13/pflag"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/pkg/types"
)

const (
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

// RestoreOptions hold all snapshot restore related fields
// Note: Please ensure DeepCopy and DeepCopyInto are properly implemented.
type RestoreOptions struct {
	Config      *RestorationConfig
	ClusterURLs types.URLsMap
	PeerURLs    types.URLs
	// Base full snapshot + delta snapshots to restore from
	BaseSnapshot  *Snapshot
	DeltaSnapList SnapList
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

// NewRestorationConfig returns the restoration config.
func NewRestorationConfig() *RestorationConfig {
	return &RestorationConfig{
		InitialCluster:           initialClusterFromName(defaultName),
		InitialClusterToken:      defaultInitialClusterToken,
		RestoreDataDir:           fmt.Sprintf("%s.etcd", defaultName),
		InitialAdvertisePeerURLs: []string{defaultInitialAdvertisePeerURLs},
		Name:                     defaultName,
		SkipHashCheck:            false,
		MaxFetchers:              defaultMaxFetchers,
		MaxCallSendMsgSize:       defaultMaxCallSendMsgSize,
		MaxRequestBytes:          defaultMaxRequestBytes,
		MaxTxnOps:                defaultMaxTxnOps,
		EmbeddedEtcdQuotaBytes:   int64(defaultEmbeddedEtcdQuotaBytes),
		AutoCompactionMode:       defaultAutoCompactionMode,
		AutoCompactionRetention:  defaultAutoCompactionRetention,
	}
}

// AddFlags adds the flags to flagset.
func (c *RestorationConfig) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.InitialCluster, "initial-cluster", c.InitialCluster, "initial cluster configuration for restore bootstrap")
	fs.StringVar(&c.InitialClusterToken, "initial-cluster-token", c.InitialClusterToken, "initial cluster token for the etcd cluster during restore bootstrap")
	fs.StringVarP(&c.RestoreDataDir, "data-dir", "d", c.RestoreDataDir, "path to the data directory")
	fs.StringArrayVar(&c.InitialAdvertisePeerURLs, "initial-advertise-peer-urls", c.InitialAdvertisePeerURLs, "list of this member's peer URLs to advertise to the rest of the cluster")
	fs.StringVar(&c.Name, "name", c.Name, "human-readable name for this member")
	fs.BoolVar(&c.SkipHashCheck, "skip-hash-check", c.SkipHashCheck, "ignore snapshot integrity hash value (required if copied from data directory)")
	fs.UintVar(&c.MaxFetchers, "max-fetchers", c.MaxFetchers, "maximum number of threads that will fetch delta snapshots in parallel")
	fs.IntVar(&c.MaxCallSendMsgSize, "max-call-send-message-size", c.MaxCallSendMsgSize, "maximum size of message that the client sends")
	fs.UintVar(&c.MaxRequestBytes, "max-request-bytes", c.MaxRequestBytes, "Maximum client request size in bytes the server will accept")
	fs.UintVar(&c.MaxTxnOps, "max-txn-ops", c.MaxTxnOps, "Maximum number of operations permitted in a transaction")
	fs.Int64Var(&c.EmbeddedEtcdQuotaBytes, "embedded-etcd-quota-bytes", c.EmbeddedEtcdQuotaBytes, "maximum backend quota for the embedded etcd used for applying delta snapshots")
	fs.StringVar(&c.AutoCompactionMode, "auto-compaction-mode", c.AutoCompactionMode, "mode for auto-compaction: 'periodic' for duration based retention. 'revision' for revision number based retention.")
	fs.StringVar(&c.AutoCompactionRetention, "auto-compaction-retention", c.AutoCompactionRetention, "Auto-compaction retention length.")
}

// Validate validates the config.
func (c *RestorationConfig) Validate() error {
	if _, err := types.NewURLsMap(c.InitialCluster); err != nil {
		return fmt.Errorf("failed creating url map for restore cluster: %v", err)
	}
	if _, err := types.NewURLs(c.InitialAdvertisePeerURLs); err != nil {
		return fmt.Errorf("failed parsing peers urls for restore cluster: %v", err)
	}
	if c.MaxCallSendMsgSize <= 0 {
		return fmt.Errorf("max call send message should be greater than zero")
	}
	if c.MaxFetchers <= 0 {
		return fmt.Errorf("max fetchers should be greater than zero")
	}
	if c.EmbeddedEtcdQuotaBytes <= 0 {
		return fmt.Errorf("Etcd Quota size for etcd must be greater than 0")
	}
	if c.AutoCompactionMode != "periodic" && c.AutoCompactionMode != "revision" {
		return fmt.Errorf("UnSupported auto-compaction-mode")
	}
	c.RestoreDataDir = path.Clean(c.RestoreDataDir)
	return nil
}

// DeepCopyInto copies the structure deeply from in to out.
func (c *RestorationConfig) DeepCopyInto(out *RestorationConfig) {
	*out = *c
	if c.InitialAdvertisePeerURLs != nil {
		c, out := &c.InitialAdvertisePeerURLs, &out.InitialAdvertisePeerURLs
		*out = make([]string, len(*c))
		for i, v := range *c {
			(*out)[i] = v
		}
	}
}

// DeepCopy returns a deeply copied structure.
func (c *RestorationConfig) DeepCopy() *RestorationConfig {
	if c == nil {
		return nil
	}

	out := new(RestorationConfig)
	c.DeepCopyInto(out)
	return out
}

func initialClusterFromName(name string) string {
	n := name
	if name == "" {
		n = defaultName
	}
	return fmt.Sprintf("%s=http://localhost:2380", n)
}

// InitIndex stores the index
type InitIndex int

// ConsistentIndex gets the index
func (i *InitIndex) ConsistentIndex() uint64 {
	return uint64(*i)
}

// Event is wrapper over etcd event to keep track of time of event
type Event struct {
	EtcdEvent *clientv3.Event `json:"etcdEvent"`
	Time      time.Time       `json:"time"`
}

// FetcherInfo stores the information about fetcher
type FetcherInfo struct {
	Snapshot  Snapshot
	SnapIndex int
}

// ApplierInfo stores the info about applier
type ApplierInfo struct {
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
func DeepCopySnapList(in SnapList) SnapList {
	out := make(SnapList, len(in))
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
