// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package member

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/gardener/etcd-backup-restore/pkg/etcdutil"

	"github.com/sirupsen/logrus"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WriteMemberIDFile atomically writes the holder identity string to the
// member-id file in the data directory.
func WriteMemberIDFile(dataDir, holderIdentity string) error {
	path := etcdutil.MemberIDFilePath(dataDir)
	// Write to a scratch file and rename it into place. os.WriteFile truncates
	// then writes, so a crash mid-write could leave the member-id file empty or
	// partial; os.Rename is atomic within a filesystem, so a concurrent reader
	// (e.g. the anti-rejoin guard) always sees either the complete old contents
	// or the complete new contents, never a half-written value.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(holderIdentity), 0600); err != nil {
		return fmt.Errorf("unable to write member ID temp file %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("unable to rename member ID file %q -> %q: %w", tmp, path, err)
	}
	return nil
}

// FormatMemberIdentity builds the holder-identity string "<memberID-hex>:<clusterID-hex>:<role>"
// that is written to the k8s Lease and to the on-disk member-id file.
func FormatMemberIdentity(memberID, clusterID uint64, isLeader bool) string {
	role := "Member"
	if isLeader {
		role = "Leader"
	}
	return strconv.FormatUint(memberID, 16) + ":" + strconv.FormatUint(clusterID, 16) + ":" + role
}

// ParseMemberIDFromIdentity parses the member ID from a holder identity string
// "<memberID-hex>:<clusterID-hex>:<role>". Also accepts a bare hex string.
func ParseMemberIDFromIdentity(identity string) (uint64, error) {
	memberIDHex, _, _ := strings.Cut(identity, ":")
	if memberIDHex == "" {
		return 0, fmt.Errorf("empty member ID in identity %q", identity)
	}
	memberID, err := strconv.ParseUint(memberIDHex, 16, 64)
	if err != nil {
		return 0, fmt.Errorf("expected hex member ID, got %q: %w", memberIDHex, err)
	}
	return memberID, nil
}

// ResolveLocalMemberID returns the local member's ID by checking the k8s lease
// first, then falling back to the on-disk member-id file. Returns false if
// neither source has an ID (fresh member).
//
// The lease fallback only applies when the lease is genuinely absent (NotFound).
// Any other API error (RBAC denial, timeout, parse failure) is propagated as-is
// so the caller can fail closed rather than silently bypass the guard.
func ResolveLocalMemberID(ctx context.Context, logger *logrus.Entry, dataDir string, k8sClient client.Client, podNamespace, memberName string) (uint64, bool, error) {
	memberID, ok, leaseErr := ReadLocalMemberIDFromLease(ctx, logger, k8sClient, podNamespace, memberName)
	if leaseErr != nil {
		if !apierrors.IsNotFound(leaseErr) {
			// Non-NotFound errors (RBAC, network, parse) are failures, not
			// absence. Fall back to the file so a temporary API blip does not
			// permanently block the member, but if the file is also absent the
			// caller must fail closed.
			logger.Warnf("could not read member ID from lease, falling back to file: %v", leaseErr)
		}
		// leaseErr == NotFound: lease does not exist yet; fall through to file.
	} else if ok {
		return memberID, true, nil
	}

	memberID, ok, err := ReadLocalMemberIDFromFile(logger, dataDir)
	if err != nil {
		return 0, false, fmt.Errorf("unable to read local member ID from file: %w", err)
	}
	if ok {
		return memberID, true, nil
	}

	logger.Info("no prior member identity found in lease or file; treating as a fresh member")
	return 0, false, nil
}

// ReadLocalMemberIDFromLease reads the member ID from the lease holder identity.
// Returns (0, false, nil) if the lease exists but has no holder identity yet.
// Returns (0, false, NotFound-err) if the lease does not exist — callers can
// use apierrors.IsNotFound to distinguish absence from a real failure.
func ReadLocalMemberIDFromLease(ctx context.Context, logger *logrus.Entry, k8sClient client.Client, podNamespace, memberName string) (uint64, bool, error) {
	if k8sClient == nil {
		return 0, false, nil
	}
	memberLease := &coordinationv1.Lease{}
	if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: podNamespace, Name: memberName}, memberLease); err != nil {
		// Return the raw error so the caller can inspect it with apierrors helpers.
		return 0, false, err
	}
	if memberLease.Spec.HolderIdentity == nil || *memberLease.Spec.HolderIdentity == "" {
		return 0, false, nil
	}
	memberID, err := ParseMemberIDFromIdentity(*memberLease.Spec.HolderIdentity)
	if err != nil {
		return 0, false, fmt.Errorf("unable to parse member ID from lease holder identity %q: %w", *memberLease.Spec.HolderIdentity, err)
	}
	logger.Infof("found member ID %s from member lease", strconv.FormatUint(memberID, 16))
	return memberID, true, nil
}

// ReadLocalMemberIDFromFile reads the member ID from the member-id file on the PV.
// Returns false (nil error) if the file does not exist.
func ReadLocalMemberIDFromFile(logger *logrus.Entry, dataDir string) (uint64, bool, error) {
	path := etcdutil.MemberIDFilePath(dataDir)
	data, err := os.ReadFile(path) // #nosec G304 -- path is derived from the operator-controlled data directory, not user input.
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("unable to read member-id file %q: %w", path, err)
	}
	memberID, err := ParseMemberIDFromIdentity(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, false, fmt.Errorf("unable to parse member ID from file %q: %w", path, err)
	}
	logger.Infof("found member ID %s from member-id file", strconv.FormatUint(memberID, 16))
	return memberID, true, nil
}
