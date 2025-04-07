// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package snapstore_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	stiface "github.com/gardener/etcd-backup-restore/pkg/snapstore/gcs"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// mockGCSClient is a mock client to be used in unit tests.
type mockGCSClient struct {
	stiface.Client
	objects     map[string]*[]byte
	objectTags  map[string]map[string]string
	prefix      string
	objectMutex sync.Mutex
}

func (m *mockGCSClient) Bucket(name string) stiface.BucketHandle {
	return &mockBucketHandle{bucket: name, client: m}
}

func (m *mockGCSClient) setTags(taggedSnapshotName string, tagMap map[string]string) {
	m.objectTags[taggedSnapshotName] = tagMap
}

func (m *mockGCSClient) deleteTags(taggedSnapshotName string) {
	delete(m.objectTags, taggedSnapshotName)
}

type mockBucketHandle struct {
	stiface.BucketHandle
	client *mockGCSClient
	bucket string
}

func (m *mockBucketHandle) Object(name string) stiface.ObjectHandle {
	return &mockObjectHandle{object: name, client: m.client}
}

func (m *mockBucketHandle) Objects(context.Context, *storage.Query) stiface.ObjectIterator {
	m.client.objectMutex.Lock()
	defer m.client.objectMutex.Unlock()
	var keys []string
	for key := range m.client.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return &mockObjectIterator{keys: keys, tags: m.client.objectTags}
}

type mockObjectHandle struct {
	stiface.ObjectHandle
	client *mockGCSClient
	object string
}

func (m *mockObjectHandle) NewReader(_ context.Context) (stiface.Reader, error) {
	m.client.objectMutex.Lock()
	defer m.client.objectMutex.Unlock()
	if value, ok := m.client.objects[m.object]; ok {
		return &mockObjectReader{reader: io.NopCloser(bytes.NewReader(*value))}, nil
	}
	return nil, fmt.Errorf("object %s not found", m.object)
}

func (m *mockObjectHandle) NewWriter(context.Context) stiface.Writer {
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
	m.client.objectMutex.Lock()
	defer m.client.objectMutex.Unlock()
	if _, ok := m.client.objects[m.object]; ok {
		delete(m.client.objects, m.object)
		delete(m.client.objectTags, m.object)
		return nil
	}
	return fmt.Errorf("object %s not found", m.object)
}

type mockObjectIterator struct {
	stiface.ObjectIterator
	tags         map[string]map[string]string
	keys         []string
	currentIndex int
}

func (m *mockObjectIterator) Next() (*storage.ObjectAttrs, error) {
	if m.currentIndex < len(m.keys) {
		name := m.keys[m.currentIndex]
		obj := &storage.ObjectAttrs{
			Name:     name,
			Metadata: m.tags[name],
		}
		m.currentIndex++
		return obj, nil
	}
	return nil, iterator.Done
}

type mockComposer struct {
	stiface.Composer
	client        *mockGCSClient
	dst           *mockObjectHandle
	objectHandles []stiface.ObjectHandle
}

func (m *mockComposer) Run(ctx context.Context) (*storage.ObjectAttrs, error) {
	dstWriter := m.dst.NewWriter(ctx)
	defer dstWriter.Close()
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
	client *mockGCSClient
	object string
	data   []byte
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
