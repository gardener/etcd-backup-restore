// Copyright (c) 2020 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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

package compressor

import (
	"fmt"

	flag "github.com/spf13/pflag"
)

// NewCompressorConfig returns the compression config.
func NewCompressorConfig() *CompressionConfig {
	return &CompressionConfig{
		Enabled:           DefaultCompression,
		CompressionPolicy: DefaultCompressionPolicy,
	}
}

// AddFlags adds the flags to flagset.
func (c *CompressionConfig) AddFlags(fs *flag.FlagSet) {

	fs.BoolVar(&c.Enabled, "compress-snapshots", c.Enabled, "whether to compress the snapshots or not")
	fs.StringVar(&c.CompressionPolicy, "compression-policy", c.CompressionPolicy, "Policy for compressing the snapshots")
}

// Validate validates the compression Config.
func (c *CompressionConfig) Validate() error {

	// if compression is not enabled then to check CompressionPolicy becomes irrelevant
	if c.Enabled == false {
		return nil
	}

	for _, policy := range []string{GzipCompressionPolicy, ZlibCompressionPolicy, LzwCompressionPolicy} {
		if c.CompressionPolicy == policy {
			return nil
		}
	}
	return fmt.Errorf("%s: Compression Policy is not supported", c.CompressionPolicy)

}
