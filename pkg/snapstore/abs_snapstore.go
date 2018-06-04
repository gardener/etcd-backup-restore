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

package snapstore

import (
	"fmt"
	"io"
	"path"
	"sort"

	"github.com/Azure/azure-sdk-for-go/storage"
)

const (
	absStorageAccount = "STORAGE_ACCOUNT"
	absStorageKey     = "STORAGE_KEY"
)

// ABSSnapStore is an ABS backed snapstore.
type ABSSnapStore struct {
	SnapStore
	prefix    string
	container *storage.Container
}

// NewABSSnapStore create new ABSSnapStore from shared configuration with specified bucket
func NewABSSnapStore(container, prefix string) (*ABSSnapStore, error) {
	storageAccount, err := GetEnvVarOrError(absStorageAccount)
	if err != nil {
		return nil, err
	}

	storageKey, err := GetEnvVarOrError(absStorageKey)
	if err != nil {
		return nil, err
	}

	client, err := storage.NewBasicClient(storageAccount, storageKey)
	if err != nil {
		return nil, fmt.Errorf("create ABS client failed: %v", err)
	}

	return GetSnapstoreFromClient(container, prefix, &client)
}

// GetSnapstoreFromClient returns a new ABS object for a given container using the supplied storageClient
func GetSnapstoreFromClient(container, prefix string, storageClient *storage.Client) (*ABSSnapStore, error) {
	client := storageClient.GetBlobService()

	// Check if supplied container exists
	containerRef := client.GetContainerReference(container)
	containerExists, err := containerRef.Exists()
	if err != nil {
		return nil, err
	}

	if !containerExists {
		return nil, fmt.Errorf("container %v does not exist", container)
	}

	return &ABSSnapStore{
		prefix:    prefix,
		container: containerRef,
	}, nil
}

// Fetch should open reader for the snapshot file from store
func (a *ABSSnapStore) Fetch(snap Snapshot) (io.ReadCloser, error) {
	blobName := path.Join(a.prefix, snap.SnapDir, snap.SnapName)
	blob := a.container.GetBlobReference(blobName)

	opts := &storage.GetBlobOptions{}
	return blob.Get(opts)
}

// List will list all snapshot files on store
func (a *ABSSnapStore) List() (SnapList, error) {
	params := storage.ListBlobsParameters{Prefix: path.Join(a.prefix) + "/"}
	resp, err := a.container.ListBlobs(params)
	if err != nil {
		return nil, err
	}

	var snapList SnapList
	for _, blob := range resp.Blobs {
		k := (blob.Name)[len(resp.Prefix):]
		s, err := ParseSnapshot(k)
		if err != nil {
			// Warning
			fmt.Printf("Invalid snapshot found. Ignoring it:%s\n", k)
		} else {
			snapList = append(snapList, s)
		}
	}
	sort.Sort(snapList)
	return snapList, nil
}

// Save will write the snapshot to store
func (a *ABSSnapStore) Save(snap Snapshot, r io.Reader) error {
	blobName := path.Join(a.prefix, snap.SnapDir, snap.SnapName)
	blob := a.container.GetBlobReference(blobName)

	putBlobOpts := storage.PutBlobOptions{}
	err := blob.CreateBlockBlobFromReader(r, &putBlobOpts)
	if err != nil {
		return fmt.Errorf("create block blob from reader failed: %v", err)
	}
	return err
}

// Delete should delete the snapshot file from store
func (a *ABSSnapStore) Delete(snap Snapshot) error {
	blobName := path.Join(a.prefix, snap.SnapDir, snap.SnapName)
	blob := a.container.GetBlobReference(blobName)

	opts := &storage.DeleteBlobOptions{}
	return blob.Delete(opts)
}
