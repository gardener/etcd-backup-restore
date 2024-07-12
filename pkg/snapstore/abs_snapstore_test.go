// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	azcontainer "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/Azure/azure-storage-blob-go/azblob"
	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"
)

func newFakeABSSnapstore() brtypes.SnapStore {
	// TODO: @renormalize new stuff
	accountName, accountKey := "dummyaccount", "dummykey"
	sharedKeyCredential, _ := azcontainer.NewSharedKeyCredential(accountName, accountKey)
	blobEndpoint, _ := ConstructABSURI(sharedKeyCredential.AccountName())
	newClient, _ := azcontainer.NewClientWithSharedKeyCredential(blobEndpoint, sharedKeyCredential, &azcontainer.ClientOptions{ClientOptions: azcore.ClientOptions{Retry: policy.RetryOptions{TryTimeout: time.Hour}}})

	a := NewABSSnapStoreFromClient(bucket, prefixV2, "/tmp", 5, brtypes.MinChunkSize, newClient)
	return a
}

// Please follow the link https://github.com/Azure/azure-pipeline-go/blob/master/pipeline/policies_test.go
// for details about details of azure policy, policy factory and pipeline

// newFakePolicyFactory creates a 'Fake' policy factory.
func newFakePolicyFactory(bucket, prefix string, objectMap map[string]*[]byte) pipeline.Factory {
	return &fakePolicyFactory{bucket, prefix, objectMap}
}

type fakePolicyFactory struct {
	bucket    string
	prefix    string
	objectMap map[string]*[]byte
}

// New initializes a Fake policy object.
func (f *fakePolicyFactory) New(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.Policy {
	return &fakePolicy{
		next:             next,
		po:               po,
		bucket:           f.bucket,
		prefix:           f.prefix,
		objectMap:        f.objectMap,
		multiPartUploads: make(map[string]map[string][]byte, 0),
	}
}

type fakePolicy struct {
	next                  pipeline.Policy
	po                    *pipeline.PolicyOptions
	bucket                string
	prefix                string
	objectMap             map[string]*[]byte
	multiPartUploads      map[string]map[string][]byte
	multiPartUploadsMutex sync.Mutex
}

// Do method is called on pipeline to process the request. This will internally call the `Do` method
// on next policies in pipeline and return the response from it.
func (p *fakePolicy) Do(ctx context.Context, request pipeline.Request) (response pipeline.Response, err error) {
	httpReq, err := http.NewRequest(request.Method, request.URL.String(), request.Body)
	if err != nil {
		return nil, err
	}
	httpReq.ContentLength = request.ContentLength

	httpResp := &http.Response{
		Request: httpReq,
	}
	switch request.Method {
	case "GET":
		object := parseObjectNamefromURL(request.URL)
		if len(object) == 0 {
			if err := p.handleContainerGetOperation(httpResp); err != nil {
				return nil, err
			}
		} else {
			p.handleBlobGetOperation(httpResp)
		}
	case "PUT":
		p.handleBlobPutOperation(httpResp)
	case "DELETE":
		p.handleDeleteObject(httpResp)
	default:
		return nil, err
	}

	return pipeline.NewHTTPResponse(httpResp), nil
}

// handleContainerGetOperation prepares response for Get operation on container.
func (p *fakePolicy) handleContainerGetOperation(w *http.Response) error {
	query := w.Request.URL.Query()
	comp := query.Get("comp")
	switch comp {
	case "":
		w.StatusCode = http.StatusOK
		w.Body = http.NoBody
		return nil
	case "list":
		return p.handleListObjects(w)
	default:
		return nil
	}
}

// handleListObjectNames responds with a blob `List` response.
func (p *fakePolicy) handleListObjects(w *http.Response) error {
	var (
		keys          []string
		blobs         []blobItem
		query         = w.Request.URL.Query()
		prefix        = query.Get("prefix")
		marker        = query.Get("marker")
		maxResultsStr = query.Get("maxresults")
		limit         = 1 //Actually for azure its 5000
		nextMaker     string
	)
	if len(maxResultsStr) != 0 {
		maxResult, err := strconv.Atoi(maxResultsStr)
		if err != nil {
			return err
		}
		limit = maxResult
	}

	for k := range p.objectMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if strings.Compare(key, marker) > 0 {
			blob := blobItem{
				Name: key,
			}
			blobs = append(blobs, blob)
			if len(blobs) == limit {
				nextMaker = key
				break
			}
		}
	}

	listBlobsFlatSegmentResponse := listBlobsFlatSegmentResponse{
		ServiceEndpoint: fmt.Sprintf("%s://%s", w.Request.URL.Scheme, w.Request.URL.Host),
		ContainerName:   w.Request.URL.EscapedPath(),
		Prefix:          prefix,
		Marker:          marker,
		Segment: blobFlatListSegment{
			BlobItems: blobs,
		},
		MaxResults: int32(limit),
		NextMarker: nextMaker,
	}

	rawXML, err := xml.MarshalIndent(listBlobsFlatSegmentResponse, "", "")
	if err != nil {
		return err
	}

	w.Body = io.NopCloser(strings.NewReader(xml.Header + string(rawXML)))
	w.StatusCode = http.StatusOK
	return nil
}

// handleBlobCreateOperation responds with a blob `Put` response.
func (p *fakePolicy) handleBlobPutOperation(w *http.Response) {
	var (
		query   = w.Request.URL.Query()
		comp    = query.Get("comp")
		blockid = query.Get("blockid")
		key     = parseObjectNamefromURL(w.Request.URL)
	)

	switch comp {
	case "block":
		content := make([]byte, w.Request.ContentLength)
		if _, err := w.Request.Body.Read(content); err != nil {
			w.StatusCode = http.StatusBadRequest
			w.Body = io.NopCloser(strings.NewReader(fmt.Sprintf("failed to read content %v", err)))
			return
		}

		p.multiPartUploadsMutex.Lock()
		blockList, ok := p.multiPartUploads[key]
		if !ok {
			blockList = make(map[string][]byte, 0)
		}
		blockList[blockid] = content
		p.multiPartUploads[key] = blockList
		p.multiPartUploadsMutex.Unlock()
		w.StatusCode = http.StatusCreated

	case "blocklist":
		content := make([]byte, w.Request.ContentLength)
		if _, err := w.Request.Body.Read(content); err != nil {
			w.StatusCode = http.StatusBadRequest
			w.Body = io.NopCloser(strings.NewReader(fmt.Sprintf("failed to read content %v", err)))
			return
		}
		blockLookupXML := strings.TrimPrefix(string(content), xml.Header)
		var blockLookupList azblob.BlockLookupList
		if err := xml.Unmarshal([]byte(blockLookupXML), &blockLookupList); err != nil {
			w.StatusCode = http.StatusBadRequest
			w.Body = io.NopCloser(strings.NewReader(fmt.Sprintf("failed to parse body %v", err)))
			return
		}
		blockContentMap := p.multiPartUploads[key]
		content = make([]byte, 0)
		for _, blockID := range blockLookupList.Latest {
			content = append(content, blockContentMap[blockID]...)
		}
		p.objectMap[key] = &content
		w.StatusCode = http.StatusCreated
	}
	w.Body = http.NoBody
}

// handleBlobGetOperation on GET request `/testContainer/testObject` responds with a `Get` response.
func (p *fakePolicy) handleBlobGetOperation(w *http.Response) {
	key := parseObjectNamefromURL(w.Request.URL)
	if _, ok := p.objectMap[key]; ok {
		w.StatusCode = http.StatusOK
		w.Body = io.NopCloser(bytes.NewReader(*p.objectMap[key]))
	} else {
		w.StatusCode = http.StatusNotFound
		w.Body = http.NoBody
	}
}

// handleDeleteObject on delete request `/testContainer/testObject` responds with a `Delete` response.
func (p *fakePolicy) handleDeleteObject(w *http.Response) {
	key := parseObjectNamefromURL(w.Request.URL)
	if _, ok := p.objectMap[key]; ok {
		delete(p.objectMap, key)
		w.StatusCode = http.StatusAccepted
	} else {
		w.StatusCode = http.StatusNotFound
	}
	w.Body = http.NoBody
}

/////////////////////////////////////////////
// Truncated azblob types for XML encoding //
/////////////////////////////////////////////

type blobItem struct {
	// XMLName is used for marshalling and is subject to removal in a future release.
	XMLName    xml.Name              `xml:"Blob"`
	Name       string                `xml:"Name"`
	Properties azblob.BlobProperties `xml:"Properties"`
}

// listBlobsFlatSegmentResponse - An enumeration of blobs
type listBlobsFlatSegmentResponse struct {
	XMLName         xml.Name            `xml:"EnumerationResults"`
	ServiceEndpoint string              `xml:"ServiceEndpoint,attr"`
	ContainerName   string              `xml:"ContainerName,attr"`
	Prefix          string              `xml:"Prefix"`
	Marker          string              `xml:"Marker"`
	MaxResults      int32               `xml:"MaxResults"`
	Segment         blobFlatListSegment `xml:"Blobs"`
	NextMarker      string              `xml:"NextMarker"`
}

// blobFlatListSegment ...
type blobFlatListSegment struct {
	// XMLName is used for marshalling and is subject to removal in a future release.
	XMLName   xml.Name   `xml:"Blobs"`
	BlobItems []blobItem `xml:"Blob"`
}
