// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

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
	CompressionPolicy string `json:"policy,omitempty"`
	Enabled           bool   `json:"enabled"`
}
