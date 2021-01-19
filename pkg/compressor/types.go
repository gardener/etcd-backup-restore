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

const (

	// GzipCompressionPolicy is constant for gzip compression algorithm.
	GzipCompressionPolicy = "gzip"
	// LzwCompressionPolicy is constant for lzw compression algorithm.
	LzwCompressionPolicy = "lzw"
	// ZlibCompressionPolicy is constant for zlib compression algorithm.
	ZlibCompressionPolicy = "zlib"

	// DefaultCompression is constant used for whether to compress the snapshots or not.
	DefaultCompression = false
	// DefaultCompressionPolicy is constant for default compression algorithm(only if compression is enabled).
	DefaultCompressionPolicy = "gzip"

	// UnCompressSnapshotExtension is used for snapshot suffix when compression is not enabled.
	UnCompressSnapshotExtension = ""
	// GzipCompressionExtension is used for snapshot suffix when compressionPolicy is gzip.
	GzipCompressionExtension = ".gz"
	// LzwCompressionExtension is used for snapshot suffix when compressionPolicy is lzw.
	LzwCompressionExtension = ".Z"
	// ZlibCompressionExtension is used for snapshot suffix when compressionPolicy is zlib.
	ZlibCompressionExtension = ".zlib"
	// Reference: https://en.wikipedia.org/wiki/List_of_archive_formats

	// LzwLiteralWidth is constant used as literal Width in lzw compressionPolicy.
	LzwLiteralWidth = 8 //[2,8]
)

// CompressionConfig holds the compression configuration.
type CompressionConfig struct {
	Enabled           bool   `json:"enabled"`
	CompressionPolicy string `json:"policy,omitempty"`
}
