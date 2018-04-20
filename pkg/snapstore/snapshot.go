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

package snapstore

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// GenerateSnapshotName preapres the snapshot name from metadata
func (s *Snapshot) GenerateSnapshotName() {
	s.SnapName = fmt.Sprintf("%s-%08d-%08d-%d", s.Kind, s.StartRevision, s.LastRevision, s.CreatedOn.Unix())
}

// GenerateSnapshotDirectory prepares the snapshot directory name from metadata
func (s *Snapshot) GenerateSnapshotDirectory() {
	s.SnapDir = fmt.Sprintf("%s-%d", "Backup", s.CreatedOn.Unix())
}

// ParseSnapshot parse <snapPath> to create snapshot structure
func ParseSnapshot(snapPath string) (*Snapshot, error) {
	var err error
	s := &Snapshot{}
	tokens := strings.Split(snapPath, "/")
	if len(tokens) <= 1 {
		return nil, fmt.Errorf("invalid snapshot name: %s", snapPath)
	}
	snapName := tokens[len(tokens)-1]
	snapDir := snapPath[:len(snapPath)-len(snapName)-1]
	tokens = strings.Split(snapName, "-")
	if len(tokens) != 4 {
		return nil, fmt.Errorf("invalid snapshot name: %s", snapName)
	}
	//parse kind
	switch tokens[0] {
	case SnapshotKindFull, "full":
		s.Kind = SnapshotKindFull
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
	//parse creation time
	unixTime, err := strconv.ParseInt(tokens[3], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid creation time: %s", tokens[3])
	}
	s.CreatedOn = time.Unix(unixTime, 0)
	s.SnapName = snapName
	s.SnapDir = snapDir
	return s, nil
}

// SnapList override sorting related function
func (s SnapList) Len() int           { return len(s) }
func (s SnapList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s SnapList) Less(i, j int) bool { return (s[i].CreatedOn.Unix() < s[j].CreatedOn.Unix()) }
