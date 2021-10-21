// Copyright (c) 2021 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
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
	"strconv"
	"strings"
	"time"

	brtypes "github.com/gardener/etcd-backup-restore/pkg/types"

	"github.com/sirupsen/logrus"
)

// NewSnapshot returns the snapshot object.
func NewSnapshot(kind string, startRevision, lastRevision int64, compressionSuffix string, isFinal bool) *brtypes.Snapshot {
	snap := &brtypes.Snapshot{
		Kind:              kind,
		StartRevision:     startRevision,
		LastRevision:      lastRevision,
		CreatedOn:         time.Now().UTC(),
		CompressionSuffix: compressionSuffix,
		IsFinal:           isFinal,
	}
	snap.GenerateSnapshotName()
	return snap
}

// ParseSnapshot parse <snapPath> to create snapshot structure
func ParseSnapshot(snapPath string) (*brtypes.Snapshot, error) {
	logrus.Debugf("Snap path: %s", snapPath)
	var err error
	var backupVersion string = ""
	s := &brtypes.Snapshot{}
	// First try if the path contains v1
	lastIndex := strings.LastIndex(snapPath, "v1/")
	if lastIndex >= 0 {
		backupVersion = backupVersionV1
	}

	// Then try if the path contains v2
	if backupVersion == "" {
		lastIndex = strings.LastIndex(snapPath, "v2/")
		if lastIndex >= 0 {
			backupVersion = backupVersionV2
		}
	}

	if backupVersion == "" {
		return nil, fmt.Errorf("invalid snapPath %v, does not contain any backupVersion", snapPath)
	}

	lastIndex = lastIndex + 3
	prefix := snapPath[:lastIndex]
	snapPath = snapPath[lastIndex:]

	tok := strings.Split(snapPath, "/")
	if len(tok) < 1 || len(tok) > 3 {
		return nil, fmt.Errorf("invalid snapshot name: %s", snapPath)
	}

	var snapName, snapDir string = "", ""
	// Get snap name from the tokens
	// Consider the token before snap name
	// If it's v1, then consider the token as snapDir
	// If it's v2, then no snapDir exist
	switch backupVersion {
	case backupVersionV1:
		if len(tok) == 3 {
			s.IsChunk = true
		}
		snapDir = tok[0]
		snapName = path.Join(tok[1:]...)
	case backupVersionV2:
		if len(tok) == 2 {
			s.IsChunk = true
		}
		snapName = path.Join(tok[0:]...)
	}

	logrus.Debugf("Prefix: %s, Snap Directory: %s, Snap Name: %s", prefix, snapDir, snapName)
	tokens := strings.Split(snapName, "-")
	if len(tokens) != 4 {
		return nil, fmt.Errorf("invalid snapshot name: %s", snapName)
	}

	//parse kind
	switch tokens[0] {
	case brtypes.SnapshotKindFull:
		s.Kind = brtypes.SnapshotKindFull
	case brtypes.SnapshotKindDelta:
		s.Kind = brtypes.SnapshotKindDelta
	default:
		return nil, fmt.Errorf("unknown snapshot kind: %s", tokens[0])
	}

	//parse start revision
	s.StartRevision, err = strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid start revision: %s", tokens[1])
	}
	//parse last revision
	s.LastRevision, err = strconv.ParseInt(tokens[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid last revision: %s", tokens[2])
	}

	if s.StartRevision > s.LastRevision {
		return nil, fmt.Errorf("last revision (%s) should be at least start revision(%s) ", tokens[2], tokens[1])
	}

	//parse creation time as well as parse the Snapshot compression suffix
	lastNameToken := strings.Split(tokens[3], "/")
	timeWithSnapSuffix := strings.Split(lastNameToken[0], ".")
	if len(timeWithSnapSuffix) >= 2 {
		if "."+timeWithSnapSuffix[1] != brtypes.FinalSuffix {
			s.CompressionSuffix = "." + timeWithSnapSuffix[1]
		}
		if "."+timeWithSnapSuffix[len(timeWithSnapSuffix)-1] == brtypes.FinalSuffix {
			s.IsFinal = true
		}
	}
	unixTime, err := strconv.ParseInt(timeWithSnapSuffix[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid creation time: %s", tokens[3])
	}
	s.CreatedOn = time.Unix(unixTime, 0).UTC()
	s.SnapName = snapName
	s.SnapDir = snapDir
	s.Prefix = prefix
	return s, nil
}
