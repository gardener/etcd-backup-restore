// Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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
	"path"

	flag "github.com/spf13/pflag"
)

const (
	backupFormatVersion = "v1"
)

// NewSnapstoreConfig returns the snapstore config.
func NewSnapstoreConfig() *Config {
	return &Config{
		MaxParallelChunkUploads: 5,
		TempDir:                 "/tmp",
	}
}

// AddFlags adds the flags to flagset.
func (c *Config) AddFlags(fs *flag.FlagSet) {
	fs.StringVar(&c.Provider, "storage-provider", c.Provider, "snapshot storage provider")
	fs.StringVar(&c.Container, "store-container", c.Container, "container which will be used as snapstore")
	fs.StringVar(&c.Prefix, "store-prefix", c.Prefix, "prefix or directory inside container under which snapstore is created")
	fs.UintVar(&c.MaxParallelChunkUploads, "max-parallel-chunk-uploads", c.MaxParallelChunkUploads, "maximum number of parallel chunk uploads allowed ")
	fs.StringVar(&c.TempDir, "snapstore-temp-directory", c.TempDir, "temporary directory for processing")
}

// Validate validates the config.
func (c *Config) Validate() error {
	if c.MaxParallelChunkUploads <= 0 {
		return fmt.Errorf("max parallel chunk uploads should be greater than zero")
	}
	return nil
}

// Complete completes the config.
func (c *Config) Complete() {
	c.Prefix = path.Join(c.Prefix, backupFormatVersion)
}
