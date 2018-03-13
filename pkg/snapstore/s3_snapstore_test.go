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

package snapstore_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	. "github.com/gardener/etcd-backup-restore/pkg/snapstore"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// Define a mock struct to be used in your unit tests of myFunc.
type mockS3Client struct {
	s3iface.S3API
	objects map[string][]byte
	prefix  string
}

// GetObject returns the object from map for mock test
func (m *mockS3Client) GetObject(in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	// Only need to return mocked response output
	out := s3.GetObjectOutput{
		Body: ioutil.NopCloser(bytes.NewReader(m.objects[*in.Key])),
	}
	return &out, nil
}

// PutObject adds the object to the map for mock test
func (m *mockS3Client) PutObject(in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	off, _ := in.Body.Seek(0, io.SeekEnd)
	in.Body.Seek(0, io.SeekStart)
	temp := make([]byte, off)
	in.Body.Read(temp)
	m.objects[*in.Key] = temp
	out := s3.PutObjectOutput{}
	return &out, nil
}

// ListObject returns the objects from map for mock test
func (m *mockS3Client) ListObjects(in *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	var contents []*s3.Object
	for key, _ := range m.objects {
		if strings.HasPrefix(key, *in.Prefix) {
			keyPtr := new(string)
			*keyPtr = key
			tempObj := &s3.Object{
				Key: keyPtr,
			}
			contents = append(contents, tempObj)
		}
	}
	out := &s3.ListObjectsOutput{
		Prefix:   in.Prefix,
		Contents: contents,
	}
	return out, nil
}

// DeleteObject deletes the object from map for mock test
func (m *mockS3Client) DeleteObject(in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	delete(m.objects, *in.Key)
	return &s3.DeleteObjectOutput{}, nil
}

var _ = Describe("S3Snapstore", func() {
	var (
		bucket       string
		snapStore    SnapStore
		snap1        Snapshot
		snap2        Snapshot
		snap3        Snapshot
		expectedVal1 string
		expectedVal2 string
		m            mockS3Client
	)
	// S3SnapStore is snapstore with local disk as backend
	BeforeEach(func() {
		bucket = "mock-bucket"
		prefix := "v1"
		now := time.Now().Unix()
		snap1 = Snapshot{
			CreatedOn:     time.Unix(now, 0),
			StartRevision: 0,
			LastRevision:  2088,
			Kind:          SnapshotKindFull,
		}
		snap2 = Snapshot{
			CreatedOn:     time.Unix(now+100, 0),
			StartRevision: 0,
			LastRevision:  1988,
			Kind:          SnapshotKindFull,
		}
		snap3 = Snapshot{
			CreatedOn:     time.Unix(now+200, 0),
			StartRevision: 0,
			LastRevision:  1958,
			Kind:          SnapshotKindFull,
		}
		snap1.GenerateSnapshotName()
		snap2.GenerateSnapshotName()
		expectedVal1 = "value1"
		expectedVal2 = "value2"
		m = mockS3Client{
			objects: map[string][]byte{
				path.Join(prefix, snap1.SnapPath): []byte(expectedVal1),
				path.Join(prefix, snap2.SnapPath): []byte(expectedVal2),
			},
			prefix: prefix,
		}
		snapStore = NewS3FromClient(bucket, prefix, &m)
	})

	Describe("Fetch snapshot", func() {
		It("fetches snapshot", func() {
			rc, err := snapStore.Fetch(snap1)
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			temp := make([]byte, len(expectedVal1))
			n, err := rc.Read(temp)
			if err != nil && err != io.EOF {
				Fail(fmt.Sprintf("%v", err))
			}
			Expect(string(temp[:n])).To(Equal(expectedVal1))
		})
	})
	Describe("Save snapshot", func() {
		It("saves snapshot", func() {
			prevLen := len(m.objects)
			err := snapStore.Save(snap3, strings.NewReader("value3"))
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			Expect(len(m.objects)).To(Equal(prevLen + 1))
		})
	})
	Describe("List snapshot", func() {
		It("gives sorted list of snapshot", func() {
			snapList, err := snapStore.List()
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			Expect(snapList.Len()).To(Equal(2))
			Expect(snapList[0].SnapPath).To(Equal(snap1.SnapPath))
		})
	})
	Describe("Delete snapshot", func() {
		It("deletes snapshot", func() {
			prevLen := len(m.objects)
			err := snapStore.Delete(snap2)
			if err != nil {
				Fail(fmt.Sprintf("%v", err))
			}
			Expect(len(m.objects)).To(Equal(prevLen - 1))
		})
	})
})
