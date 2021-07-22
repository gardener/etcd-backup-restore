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

package miscellaneous

import (
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/gardener/etcd-backup-restore/pkg/types"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	snapList types.SnapList
	ds       DummyStore
)

const (
	generatedSnaps = 20
)

var _ = Describe("Filtering snapshots", func() {
	BeforeEach(func() {
		snapList = generateSnapshotList(generatedSnaps)
		ds = NewDummyStore(snapList)

	})
	It("should return the whole snaplist", func() {
		snaps, err := GetFilteredBackups(&ds, -1, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(len(snaps)).To(Equal(len(snapList)))
	})
	It("should get the last 3 snapshots and its deltas", func() {
		n := 3
		snaps, err := GetFilteredBackups(&ds, n, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(len(snaps)).To(Equal(n * 2))
		expectedSnapID := 0
		for i := 0; i < n; i++ {
			if reflect.DeepEqual(snaps[i].Kind, types.SnapshotKindFull) {
				Expect(snaps[i].SnapName).To(Equal(fmt.Sprintf("%s-%d", types.SnapshotKindFull, expectedSnapID)))
				Expect(snaps[i+1].SnapName).To(Equal(fmt.Sprintf("%s-%d", types.SnapshotKindDelta, expectedSnapID)))
				expectedSnapID++
			}
		}
	})
	It("should get the last backups created in the past 5 days", func() {
		n := 5
		backups, err := GetFilteredBackups(&ds, n, func(snap types.Snapshot) bool {
			return snap.CreatedOn.After(time.Now().UTC().AddDate(0, 0, -n))
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(len(backups)).To(Equal(n * 2))
		for i := 0; i < n; i++ {
			if reflect.DeepEqual(backups[i].Kind, types.SnapshotKindFull) {
				backups[i].CreatedOn.After(time.Now().UTC().AddDate(0, 0, -n))
			}
		}
	})
})

func generateSnapshotList(n int) types.SnapList {
	snapList := types.SnapList{}
	for i := 0; i < n; i++ {
		fullSnap := &types.Snapshot{
			SnapName:  fmt.Sprintf("%s-%d", types.SnapshotKindFull, i),
			Kind:      types.SnapshotKindFull,
			CreatedOn: time.Now().UTC().AddDate(0, 0, -i),
		}
		deltaSnap := &types.Snapshot{
			SnapName:  fmt.Sprintf("%s-%d", types.SnapshotKindDelta, i),
			Kind:      types.SnapshotKindDelta,
			CreatedOn: time.Now().UTC().AddDate(0, 0, -i),
		}
		snapList = append(snapList, fullSnap, deltaSnap)
	}
	return snapList
}

type DummyStore struct {
	SnapList types.SnapList
}

func NewDummyStore(snapList types.SnapList) DummyStore {
	return DummyStore{SnapList: snapList}
}

func (ds *DummyStore) List() (types.SnapList, error) {
	return ds.SnapList, nil
}

func (ds *DummyStore) Delete(s brtypes.Snapshot) error {
	return nil
}

func (ds *DummyStore) Save(snap brtypes.Snapshot, rc io.ReadCloser) error {
	return nil
}

func (ds *DummyStore) Fetch(snap brtypes.Snapshot) (io.ReadCloser, error) {
	return nil, nil
}
