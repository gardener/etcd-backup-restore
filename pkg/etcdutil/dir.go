// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package etcdutil

import "path/filepath"

const (
	// MemberIDFileName is the name of the file that caches the member identity on the PV.
	MemberIDFileName = "member-id"
)

// MemberDir returns the member subdirectory for the given etcd data directory.
func MemberDir(dataDir string) string { return filepath.Join(dataDir, "member") }

// WALDir returns the WAL subdirectory for the given etcd data directory.
func WALDir(dataDir string) string { return filepath.Join(MemberDir(dataDir), "wal") }

// SnapDir returns the snap subdirectory for the given etcd data directory.
func SnapDir(dataDir string) string { return filepath.Join(MemberDir(dataDir), "snap") }

// BackendDBPath returns the boltdb backend file path for the given etcd data directory.
func BackendDBPath(dataDir string) string { return filepath.Join(SnapDir(dataDir), "db") }

// MemberIDFilePath returns the path of the member-id file for the given data directory.
func MemberIDFilePath(dataDir string) string { return filepath.Join(dataDir, MemberIDFileName) }
