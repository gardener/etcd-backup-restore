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

package cmd

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"

	ver "github.com/gardener/etcd-backup-restore/pkg/version"
	"github.com/magiconair/properties"
	"github.com/spf13/cobra"
)

// initializeSnapstoreFlags adds the snapstore related flags to <cmd>
func initializeSnapstoreFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(&storageProvider, "storage-provider", "", "snapshot storage provider")
	cmd.Flags().StringVar(&storageContainer, "store-container", "", "container which will be used as snapstore")
	cmd.Flags().StringVar(&storagePrefix, "store-prefix", "", "prefix or directory inside container under which snapstore is created")
	cmd.Flags().IntVar(&maxParallelChunkUploads, "max-parallel-chunk-uploads", 5, "maximum number of parallel chunk uploads allowed ")
	cmd.Flags().StringVar(&snapstoreTempDir, "snapstore-temp-directory", "/tmp", "temporary directory for processing")
	cmd.Flags().StringVar(&snapstoreCredentialsFile, "snapstore-credentials-file", "", "file containing snapstore credentials properties")
	cmd.Flags().StringVar(&snapstoreCredentialsDir, "snapstore-credentials-dir", "", "directory containing snapstore credentials properties as files")
}

func printVersionInfo() {
	logger.Infof("etcd-backup-restore Version: %s", ver.Version)
	logger.Infof("Git SHA: %s", ver.GitSHA)
	logger.Infof("Go Version: %s", runtime.Version())
	logger.Infof("Go OS/Arch: %s/%s", runtime.GOOS, runtime.GOARCH)
}

// GetSnapStoreConfig retrieves a Properties object either from a given credentials file or directory.
func GetSnapStoreConfig() (*properties.Properties, error) {
	if snapstoreCredentialsFile != "" {
		return readCredentialsFile(snapstoreCredentialsFile)
	}
	if snapstoreCredentialsDir != "" {
		return readCredentialsDir(snapstoreCredentialsDir)
	}
	return nil, nil
}

func readCredentialsFile(file string) (*properties.Properties, error) {
	return properties.LoadFile(file, properties.UTF8)
}

func readCredentialsDir(dir string) (*properties.Properties, error) {
	ok, err := isDirectory(dir)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%s is not a directory", dir)
	}
	props := properties.NewProperties()
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.Mode().IsRegular() {
			path := filepath.Join(dir, f.Name())
			bytes, err := ioutil.ReadFile(path)
			if err != nil {
				return nil, err
			}
			s := string(bytes)
			props.Set(f.Name(), s)
		}
	}
	return props, nil
}

func isDirectory(path string) (bool, error) {
	fileInfo, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return fileInfo.Mode().IsDir(), err
}
