// Copyright 2019 The etcd Authors
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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// LabelSucceeded is a metric label indicating whether associated metric
	// series is for success or failure.
	LabelSucceeded = "succeeded"
	// ValueSucceededTrue is value True for metric label succeeded.
	ValueSucceededTrue = "true"
	// ValueSucceededFalse is value False for metric label failed.
	ValueSucceededFalse = "false"
	// LabelKind is a metrics label indicates kind of snapshot associated with metric.
	LabelKind = "kind"

	namespaceEtcdBR   = "etcdbr"
	subsystemSnapshot = "snapshot"
)

var (
	// GCSnapshotCounter is metric to count the garbage collected snapshots.
	GCSnapshotCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespaceEtcdBR,
			Subsystem: subsystemSnapshot,
			Name:      "gc_total",
			Help:      "Total number of garbage collected snapshots.",
		},
		[]string{LabelKind, LabelSucceeded},
	)

	// LatestSnapshotRevision is metric to expose latest snapshot revision.
	LatestSnapshotRevision = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespaceEtcdBR,
			Subsystem: subsystemSnapshot,
			Name:      "latest_revision",
			Help:      "Revision number of latest snapshot taken.",
		},
		[]string{LabelKind},
	)
	// LatestSnapshotTimestamp is metric expose latest snapshot timestamp.
	LatestSnapshotTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespaceEtcdBR,
			Subsystem: subsystemSnapshot,
			Name:      "latest_timestamp",
			Help:      "Timestamp of latest snapshot taken.",
		},
		[]string{LabelKind},
	)

	// SnapshotDurationSeconds is metric to expose the duration required to save snapshot in seconds.
	SnapshotDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespaceEtcdBR,
			Subsystem: subsystemSnapshot,
			Name:      "duration_seconds",
			Help:      "Total latency distribution of saving snapshot to object store.",
		},
		[]string{LabelKind, LabelSucceeded},
	)
	// ValidationDurationSeconds is metric to expose the duration required to validate the etcd data directory in seconds.
	ValidationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespaceEtcdBR,
			Name:      "validation_duration_seconds",
			Help:      "Total latency distribution of validating data directory.",
		},
		[]string{LabelSucceeded},
	)
	// RestorationDurationSeconds is metric to expose the duration required to restore the data directory from snapshots in seconds.
	RestorationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespaceEtcdBR,
			Name:      "restoration_duration_seconds",
			Help:      "Total latency distribution of restoring from snapshot.",
		},
		[]string{LabelSucceeded},
	)
	// DefragmentationDurationSeconds is metric to expose duration required to defragment snapshot.
	DefragmentationDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespaceEtcdBR,
			Name:      "defragmentation_duration_seconds",
			Help:      "Total latency distribution of defragmentation of etcd.",
		},
		[]string{LabelSucceeded},
	)
)

func init() {
	// Metrics have to be registered to be exposed:
	prometheus.MustRegister(GCSnapshotCounter)

	prometheus.MustRegister(LatestSnapshotRevision)
	prometheus.MustRegister(LatestSnapshotTimestamp)

	prometheus.MustRegister(SnapshotDurationSeconds)
	prometheus.MustRegister(RestorationDurationSeconds)
	prometheus.MustRegister(ValidationDurationSeconds)
	prometheus.MustRegister(DefragmentationDurationSeconds)
}
