// Copyright © 2018 The Gardener Authors.
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

package snapshotter

import (
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
	"github.com/sirupsen/logrus"
)

// Snapshotter is a struct for etcd snapshot taker
type Snapshotter struct {
	logger    *logrus.Logger
	endpoints []string
	schedule  string
	store     snapstore.SnapStore
}

//NewSnapshotter returns the snpashotter object
func NewSnapshotter(endpoints []string, schedule string, store snapstore.SnapStore, logger *logrus.Logger) *Snapshotter {
	return &Snapshotter{
		logger:    logger,
		schedule:  schedule,
		endpoints: endpoints,
		store:     store,
	}
}

// Run process loop for scheduled backup
func (ssr *Snapshotter) Run(stopCh <-chan struct{}) {
	select {
	case <-stopCh:
		return
	}
}
