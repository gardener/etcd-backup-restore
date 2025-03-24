// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

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
	if !c.Enabled {
		return nil
	}

	for _, policy := range []string{GzipCompressionPolicy, ZlibCompressionPolicy, LzwCompressionPolicy} {
		if c.CompressionPolicy == policy {
			return nil
		}
	}
	return fmt.Errorf("%s: Compression Policy is not supported", c.CompressionPolicy)

}
