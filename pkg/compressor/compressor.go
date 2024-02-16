// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package compressor

import (
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"fmt"
	"io"

	"github.com/sirupsen/logrus"
)

// CompressSnapshot takes uncompressed data as input and compress the data according to Compression Policy
// and write the compressed data into one end of pipe.
func CompressSnapshot(data io.ReadCloser, compressionPolicy string) (io.ReadCloser, error) {
	pReader, pWriter := io.Pipe()

	var gWriter io.WriteCloser
	logger := logrus.New().WithField("actor", "compressor")
	logger.Infof("start compressing the snapshot using %v Compression Policy", compressionPolicy)

	switch compressionPolicy {
	case GzipCompressionPolicy:
		gWriter = gzip.NewWriter(pWriter)

	case LzwCompressionPolicy:
		gWriter = lzw.NewWriter(pWriter, lzw.LSB, LzwLiteralWidth)

	case ZlibCompressionPolicy:
		gWriter = zlib.NewWriter(pWriter)

	// It is actually unreachable but just to be on safe side:
	// for unsupported CompressionPolicy return the error
	default:
		return nil, fmt.Errorf("unsupported Compression Policy")

	}

	go func() {
		var err error
		var n int64
		defer pWriter.CloseWithError(err)
		defer gWriter.Close()
		defer data.Close()
		n, err = io.Copy(gWriter, data)
		if err != nil {
			logger.Errorf("compression failed: %v", err)
			return
		}
		logger.Infof("Total written bytes: %v", n)
	}()

	return pReader, nil
}

// DecompressSnapshot take compressed data and compressionPolicy as input and
// it decompresses the data according to compression Policy and return uncompressed data.
func DecompressSnapshot(data io.ReadCloser, compressionPolicy string) (io.ReadCloser, error) {
	var deCompressedData io.ReadCloser
	var err error

	logger := logrus.New().WithField("actor", "de-compressor")
	logger.Infof("start decompressing the snapshot with %v compressionPolicy", compressionPolicy)

	switch compressionPolicy {
	case ZlibCompressionPolicy:
		deCompressedData, err = zlib.NewReader(data)
		if err != nil {
			logger.Errorf("unable to decompress: %v", err)
			return data, err
		}

	case GzipCompressionPolicy:
		deCompressedData, err = gzip.NewReader(data)
		if err != nil {
			logger.Errorf("unable to decompress: %v", err)
			return data, err
		}

	case LzwCompressionPolicy:
		deCompressedData = lzw.NewReader(data, lzw.LSB, LzwLiteralWidth)

	// It is actually unreachable but just to be on safe side:
	// for unsupported CompressionPolicy return the same data with error
	default:
		return data, fmt.Errorf("unsupported Compression Policy")
	}

	return deCompressedData, nil
}

// GetCompressionSuffix returns the suffix for snapshot w.r.t Compression Policy
// if compression is not enabled, it will simply return UnCompressSnapshotExtension(empty string).
func GetCompressionSuffix(compressionEnabled bool, compressionPolicy string) (string, error) {

	if !compressionEnabled {
		return UnCompressSnapshotExtension, nil
	}

	switch compressionPolicy {
	case ZlibCompressionPolicy:
		return ZlibCompressionExtension, nil

	case LzwCompressionPolicy:
		return LzwCompressionExtension, nil

	case GzipCompressionPolicy:
		return GzipCompressionExtension, nil

	// unreachable but just to be on safe side:
	// for unsupported CompressionPolicy return the error
	default:
		return "", fmt.Errorf("unsupported Compression Policy")

	}
}

// IsSnapshotCompressed is helpful in determining whether the snapshot is compressed or not.
// it will return boolean, compressionPolicy corresponding to compressionSuffix and error.
func IsSnapshotCompressed(compressionSuffix string) (bool, string, error) {

	switch compressionSuffix {
	case ZlibCompressionExtension:
		return true, ZlibCompressionPolicy, nil

	case GzipCompressionExtension:
		return true, GzipCompressionPolicy, nil

	case LzwCompressionExtension:
		return true, LzwCompressionPolicy, nil

	case UnCompressSnapshotExtension:
		return false, "", nil

	// actually unreachable but just to be on safe side:
	// for unsupported CompressionPolicy return the error
	default:
		return false, "", fmt.Errorf("unsupported Compression Policy")
	}
}
