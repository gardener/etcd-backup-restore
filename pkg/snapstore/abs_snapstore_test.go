// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	azblob "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	azcontainer "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
)

type fakeABSContainerClient struct {
	objects map[string]*[]byte
	prefix  string
	// a map of blobClients so new clients created to a particular blob refer to the same blob
	blobClients map[string]*fakeBlockBlobClient
}

// NewListBlobsFlatPager will directly return a usable instance of *runtime.Pager[azcontainer.ListBlobsFlatResponse]. Returns one page per snapshot.
func (c *fakeABSContainerClient) NewListBlobsFlatPager(o *azcontainer.ListBlobsFlatOptions) *runtime.Pager[azcontainer.ListBlobsFlatResponse] {
	names := []string{}
	// Prefix has to be respected in the mock
	for name := range c.objects {
		if strings.HasPrefix(name, *o.Prefix) {
			names = append(names, name)
		}
	}

	// keeps count of which page was last returned
	index, count := 0, len(names)

	return runtime.NewPager(runtime.PagingHandler[azcontainer.ListBlobsFlatResponse]{
		More: func(_ container.ListBlobsFlatResponse) bool {
			if index < count {
				return true
			}
			return false
		},
		// Return one page for each blob
		Fetcher: func(_ context.Context, page *container.ListBlobsFlatResponse) (container.ListBlobsFlatResponse, error) {
			blobItems := []*container.BlobItem{{Name: &names[index]}}
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
func (c *fakeABSContainerClient) NewBlockBlobClient(blobName string) snapstore.AzureBlockBlobClienter {
	// The source of truth is the object map, an (older) instance of a client might exist for an object that has been deleted
	if _, ok := c.objects[blobName]; ok {
		// Check if a client was made previously (it might have contents staged)
		if _, ok := c.blobClients[blobName]; ok {
			return c.blobClients[blobName]
		}
	}

	// New client if a client was not made before, or if it the snapshot does not exist
	c.blobClients[blobName] = &fakeBlockBlobClient{name: blobName, objects: c.objects}
	return c.blobClients[blobName]
}

type fakeBlockBlobClient struct {
	name    string
	objects map[string]*[]byte
	staging []byte
	mutex   sync.Mutex
}

// DownloadStream returns the only field that is accessed from the response, which is the io.ReaderCloser to the data
func (c *fakeBlockBlobClient) DownloadStream(ctx context.Context, o *azblob.DownloadStreamOptions) (azblob.DownloadStreamResponse, error) {
	if _, ok := c.objects[c.name]; !ok {
		return azblob.DownloadStreamResponse{}, fmt.Errorf("the blob does not exist")
	}

	return azblob.DownloadStreamResponse{
		DownloadResponse: blob.DownloadResponse{
			Body: io.NopCloser(bytes.NewReader(*c.objects[c.name])),
		},
	}, nil
}

// Delete deletes the blobs from the objectMap
func (c *fakeBlockBlobClient) Delete(ctx context.Context, o *azblob.DeleteOptions) (azblob.DeleteResponse, error) {
	if _, ok := c.objects[c.name]; ok {
		delete(c.objects, c.name)
	} else {
		return azblob.DeleteResponse{}, fmt.Errorf("object with name %s not found", c.name)
	}

	return azblob.DeleteResponse{}, nil
}

// CommitBlockList "commits" the blocks in the "staging" area
func (c *fakeBlockBlobClient) CommitBlockList(ctx context.Context, base64BlockIDs []string, options *blockblob.CommitBlockListOptions) (blockblob.CommitBlockListResponse, error) {
	c.objects[c.name] = &c.staging
	c.staging = []byte{}
	return blockblob.CommitBlockListResponse{}, nil
}

// StageBlock "uploads" to the "staging" area for the blobs
func (c *fakeBlockBlobClient) StageBlock(ctx context.Context, base64BlockID string, body io.ReadSeekCloser, options *blockblob.StageBlockOptions) (blockblob.StageBlockResponse, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	contents := bytes.NewBuffer([]byte{})
	_, err := io.Copy(contents, body)
	if err != nil {
		return blockblob.StageBlockResponse{}, fmt.Errorf("error while staging the block: %w", err)
	}
	defer body.Close()
	// TODO: @renormalize the blockID should be used to sort the order of the bytes
	c.staging = append(c.staging, contents.Bytes()...)
	return blockblob.StageBlockResponse{}, nil
}
