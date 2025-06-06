// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"k8s.io/utils/ptr"
)

type fakeABSContainerClient struct {
	objects    map[string]*[]byte
	objectTags map[string]map[string]string
	// a map of blobClients so new clients created to a particular blob refer to the same blob
	blobClients map[string]*fakeBlockBlobClient
	prefix      string
	mutex       sync.Mutex
}

// NewListBlobsFlatPager will directly return a usable instance of *runtime.Pager[azcontainer.ListBlobsFlatResponse]. Returns one page per snapshot.
func (c *fakeABSContainerClient) NewListBlobsFlatPager(o *container.ListBlobsFlatOptions) *runtime.Pager[container.ListBlobsFlatResponse] {
	names := []string{}
	// Prefix has to be respected in the mock
	for name := range c.objects {
		if strings.HasPrefix(name, *o.Prefix) {
			names = append(names, name)
		}
	}

	blobTagSetMap := make(map[string][]*container.BlobTag)
	for blobName, blobTags := range c.objectTags {
		for key, value := range blobTags {
			blobTagSetMap[blobName] = append(blobTagSetMap[blobName], &container.BlobTag{
				Key:   ptr.To(key),
				Value: ptr.To(value),
			})
		}
	}

	// keeps count of which page was last returned
	index, count := 0, len(names)

	return runtime.NewPager(runtime.PagingHandler[container.ListBlobsFlatResponse]{
		More: func(_ container.ListBlobsFlatResponse) bool {
			return index < count
		},
		// Return one page for each blob
		Fetcher: func(_ context.Context, _ *container.ListBlobsFlatResponse) (container.ListBlobsFlatResponse, error) {
			blobItems := []*container.BlobItem{
				{
					Name:       &names[index],
					Properties: &container.BlobProperties{},
					BlobTags: &container.BlobTags{
						BlobTagSet: blobTagSetMap[names[index]],
					},
				},
			}
			index++
			return container.ListBlobsFlatResponse{
				ListBlobsFlatSegmentResponse: container.ListBlobsFlatSegmentResponse{
					Segment: &container.BlobFlatListSegment{
						BlobItems: blobItems,
					},
				},
			}, nil
		},
	})
}

// NewBlockBlobClient will return a mocked instance of block blob client
func (c *fakeABSContainerClient) NewBlockBlobClient(blobName string) snapstore.AzureBlockBlobClientI {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// The source of truth is the object map, an (older) instance of a client might exist for an object that has been deleted
	if _, ok := c.objects[blobName]; ok {
		// Check if a client was made previously for a blob that exists
		if _, ok := c.blobClients[blobName]; ok {
			return c.blobClients[blobName]
		}
	}

	// Check if a client was made before for a blob that does not exist yet (contents are staged)
	if _, ok := c.blobClients[blobName]; ok {
		return c.blobClients[blobName]
	}

	// New client if a client was not made before, or if the snapshot does not exist
	c.blobClients[blobName] = &fakeBlockBlobClient{name: blobName,
		deleteFn: func() {
			delete(c.objects, blobName)
			delete(c.objectTags, blobName)
		},
		checkExistenceFn: func() bool {
			_, ok := c.objects[blobName]
			return ok
		},
		commitFn: func(b *[]byte) {
			c.objects[blobName] = b
		},
		getContentFn: func() *[]byte {
			return c.objects[blobName]
		},
		staging: make(map[string][]byte),
	}
	return c.blobClients[blobName]
}

func (c *fakeABSContainerClient) setTags(taggedSnapshotName string, tagMap map[string]string) {
	c.objectTags[taggedSnapshotName] = tagMap
}

func (c *fakeABSContainerClient) deleteTags(taggedSnapshotName string) {
	delete(c.objectTags, taggedSnapshotName)
}

type fakeBlockBlobClient struct {
	staging          map[string][]byte
	deleteFn         func()
	checkExistenceFn func() bool
	commitFn         func(*[]byte)
	getContentFn     func() *[]byte
	name             string
	mutex            sync.Mutex
}

// DownloadStream returns the only field that is accessed from the response, which is the io.ReadCloser to the data
func (c *fakeBlockBlobClient) DownloadStream(_ context.Context, _ *blob.DownloadStreamOptions) (blob.DownloadStreamResponse, error) {
	if ok := c.checkExistenceFn(); !ok {
		return blob.DownloadStreamResponse{}, fmt.Errorf("the blob does not exist")
	}

	return blob.DownloadStreamResponse{
		DownloadResponse: blob.DownloadResponse{
			Body: io.NopCloser(bytes.NewReader(*c.getContentFn())),
		},
	}, nil
}

// Delete deletes the blobs from the objectMap
func (c *fakeBlockBlobClient) Delete(_ context.Context, _ *blob.DeleteOptions) (blob.DeleteResponse, error) {
	if ok := c.checkExistenceFn(); !ok {
		return blob.DeleteResponse{}, fmt.Errorf("object with name %s not found", c.name)
	}

	c.deleteFn()

	return blob.DeleteResponse{}, nil
}

// CommitBlockList "commits" the blocks in the "staging" area
func (c *fakeBlockBlobClient) CommitBlockList(_ context.Context, _ []string, _ *blockblob.CommitBlockListOptions) (blockblob.CommitBlockListResponse, error) {
	keys := []string{}
	for key := range c.staging {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	contents := []byte{}
	for _, key := range keys {
		contents = append(contents, c.staging[key]...)
	}

	c.commitFn(&contents)
	c.staging = make(map[string][]byte)

	return blockblob.CommitBlockListResponse{}, nil
}

// StageBlock "uploads" to the "staging" area for the blobs
func (c *fakeBlockBlobClient) StageBlock(_ context.Context, base64BlockID string, body io.ReadSeekCloser, _ *blockblob.StageBlockOptions) (blockblob.StageBlockResponse, error) {
	contents := bytes.NewBuffer([]byte{})
	if _, err := io.Copy(contents, body); err != nil {
		return blockblob.StageBlockResponse{}, fmt.Errorf("error while staging the block: %w", err)
	}

	decodedBytes, err := base64.StdEncoding.DecodeString(base64BlockID)
	if err != nil {
		return blockblob.StageBlockResponse{}, fmt.Errorf("unable to decode string into bytes")
	}
	decoded := string(decodedBytes)

	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.staging[decoded] = contents.Bytes()

	return blockblob.StageBlockResponse{}, nil
}
