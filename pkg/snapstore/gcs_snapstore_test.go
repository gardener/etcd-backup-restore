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

package snapstore_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/googleapis/google-cloud-go-testing/storage/stiface"
	"google.golang.org/api/iterator"
)

// mockGCSClient is a mock client to be used in unit tests.
type mockGCSClient struct {
	stiface.Client
	objects     map[string]*[]byte
	prefix      string
	objectMutex sync.Mutex
}

func (m *mockGCSClient) Bucket(name string) stiface.BucketHandle {
	return &mockBucketHandle{bucket: name, client: m}
}

type mockBucketHandle struct {
	stiface.BucketHandle
	bucket string
	client *mockGCSClient
}

func (m *mockBucketHandle) Object(name string) stiface.ObjectHandle {
	return &mockObjectHandle{object: name, client: m.client}
}

func (m *mockBucketHandle) Objects(context.Context, *storage.Query) stiface.ObjectIterator {
	if networkTimeoutFlag {
		return nil
	}

	var keys []string
	for key, _ := range m.client.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return &mockObjectIterator{keys: keys}
}

type mockObjectHandle struct {
	stiface.ObjectHandle
	object string
	client *mockGCSClient
}

func (m *mockObjectHandle) NewReader(ctx context.Context) (stiface.Reader, error) {
	if networkTimeoutFlag {
		return nil, fmt.Errorf("network timeout for NewReader()")
	}

	if value, ok := m.client.objects[m.object]; ok {
		return &mockObjectReader{reader: ioutil.NopCloser(bytes.NewReader(*value))}, nil
	}
	return nil, fmt.Errorf("object %s not found", m.object)
}

func (m *mockObjectHandle) NewWriter(context.Context) stiface.Writer {
	if networkTimeoutFlag {
		return nil
	}
	return &mockObjectWriter{object: m.object, client: m.client}
}

func (m *mockObjectHandle) ComposerFrom(objects ...stiface.ObjectHandle) stiface.Composer {
	return &mockComposer{
		objectHandles: objects,
		client:        m.client,
		dst:           m,
	}
}

func (m *mockObjectHandle) Delete(context.Context) error {
	if networkTimeoutFlag {
		return fmt.Errorf("network timeout for Delete()")
	}

	if _, ok := m.client.objects[m.object]; ok {
		delete(m.client.objects, m.object)
		return nil
	}
	return fmt.Errorf("object %s not found", m.object)
}

type mockObjectIterator struct {
	stiface.ObjectIterator
	currentIndex int
	keys         []string
}

func (m *mockObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if m.currentIndex < len(m.keys) {
		obj := &storage.ObjectAttrs{
			Name: m.keys[m.currentIndex],
		}
		m.currentIndex++
		return obj, nil
	}
	return nil, iterator.Done
}

type mockComposer struct {
	stiface.Composer
	objectHandles []stiface.ObjectHandle
	client        *mockGCSClient
	dst           *mockObjectHandle
}

func (m *mockComposer) Run(ctx context.Context) (*storage.ObjectAttrs, error) {
	dstWriter := m.dst.NewWriter(ctx)
	for _, obj := range m.objectHandles {
		r, err := obj.NewReader(ctx)
		if err != nil {
			return nil, err
		}
		if _, err := io.Copy(dstWriter, r); err != nil {
			return nil, err
		}
	}
	return &storage.ObjectAttrs{
		Name: m.dst.object,
	}, nil
}

type mockObjectReader struct {
	stiface.Reader
	reader io.ReadCloser
}

func (m *mockObjectReader) Read(p []byte) (n int, err error) {
	return m.reader.Read(p)
}

func (m *mockObjectReader) Close() error {
	return m.reader.Close()
}

type mockObjectWriter struct {
	stiface.Writer
	object string
	data   []byte
	client *mockGCSClient
}

func (m *mockObjectWriter) Write(p []byte) (n int, err error) {
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *mockObjectWriter) Close() error {
	m.client.objectMutex.Lock()
	m.client.objects[m.object] = &m.data
	m.client.objectMutex.Unlock()
	return nil
}
