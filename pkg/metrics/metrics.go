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
	"sort"

	"github.com/gardener/etcd-backup-restore/pkg/snapstore"
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
	labels = map[string][]string{
		LabelKind: {
			snapstore.SnapshotKindFull,
			snapstore.SnapshotKindDelta,
			snapstore.SnapshotKindChunk,
		},
		LabelSucceeded: {
			ValueSucceededFalse,
			ValueSucceededTrue,
		},
	}

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

	// LatestSnapshotTimestamp is metric to expose latest snapshot timestamp.
	LatestSnapshotTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespaceEtcdBR,
			Subsystem: subsystemSnapshot,
			Name:      "latest_timestamp",
			Help:      "Timestamp of latest snapshot taken.",
		},
		[]string{LabelKind},
	)

	// SnapshotRequired is metric to expose snapshot required flag.
	SnapshotRequired = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespaceEtcdBR,
			Subsystem: subsystemSnapshot,
			Name:      "required",
			Help:      "Indicates whether a snapshot is required to be taken.",
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

// generateLabelCombinations generates combinations of label values for metrics
func generateLabelCombinations(labelValues map[string][]string) []map[string]string {
	labels := make([]string, len(labelValues))
	valuesList := make([][]string, len(labelValues))
	valueCounts := make([]int, len(labelValues))
	i := 0
	for label := range labelValues {
		labels[i] = label
		i++
	}
	sort.Strings(labels)
	for i, label := range labels {
		values := make([]string, len(labelValues[label]))
		for j := range labelValues[label] {
			values[j] = labelValues[label][j]
		}
		valuesList[i] = values
		valueCounts[i] = len(values)
	}
	combinations := getCombinations(valuesList)

	output := make([]map[string]string, len(combinations))
	for i, combination := range combinations {
		labelVals := make(map[string]string, len(labels))
		for j := 0; j < len(labels); j++ {
			labelVals[labels[j]] = combination[j]
		}
		output[i] = labelVals
	}
	return output
}

// getCombinations returns combinations of slice of string slices
func getCombinations(valuesList [][]string) [][]string {
	if len(valuesList) == 0 {
		return [][]string{}
	} else if len(valuesList) == 1 {
		return wrapInSlice(valuesList[0])
	}

	return cartesianProduct(wrapInSlice(valuesList[0]), getCombinations(valuesList[1:]))
}

// cartesianProduct combines two slices of slice of strings while also
// combining the sub-slices of strings into a single string
// Ex:
// a => [[p,q],[r,s]]
// b => [[1,2],[3,4]]
// Output => [[p,q,1,2],[p,q,3,4],[r,s,1,2],[r,s,3,4]]
func cartesianProduct(a [][]string, b [][]string) [][]string {
	output := make([][]string, len(a)*len(b))
	for i := 0; i < len(a); i++ {
		for j := 0; j < len(b); j++ {
			arr := make([]string, len(a[i])+len(b[j]))
			ctr := 0
			for ii := 0; ii < len(a[i]); ii++ {
				arr[ctr] = a[i][ii]
				ctr++
			}
			for jj := 0; jj < len(b[j]); jj++ {
				arr[ctr] = b[j][jj]
				ctr++
			}
			output[(i*len(b))+j] = arr
		}
	}
	return output
}

// wrapInSlice is a helper function to wrap a slice of strings within
// a slice of slices of strings
// Ex: [p,q,r] -> [[p],[q],[r]]
func wrapInSlice(s []string) [][]string {
	output := make([][]string, len(s))
	for i := 0; i < len(output); i++ {
		elem := make([]string, 1)
		elem[0] = s[i]
		output[i] = elem
	}
	return output
}

func init() {
	// Metrics have to be initialized to zero-values
	// GCSnapshotCounter
	gcSnapshotCounterLabelValues := map[string][]string{
		LabelKind:      labels[LabelKind],
		LabelSucceeded: labels[LabelSucceeded],
	}
	gcSnapshotCounterCombinations := generateLabelCombinations(gcSnapshotCounterLabelValues)
	for _, combination := range gcSnapshotCounterCombinations {
		GCSnapshotCounter.With(prometheus.Labels(combination))
	}

	// LatestSnapshotRevision
	latestSnapshotRevisionLabelValues := map[string][]string{
		LabelKind: labels[LabelKind],
	}
	latestSnapshotRevisionCombinations := generateLabelCombinations(latestSnapshotRevisionLabelValues)
	for _, combination := range latestSnapshotRevisionCombinations {
		LatestSnapshotRevision.With(prometheus.Labels(combination))
	}

	// LatestSnapshotTimestamp
	latestSnapshotTimestampLabelValues := map[string][]string{
		LabelKind: labels[LabelKind],
	}
	latestSnapshotTimestampCombinations := generateLabelCombinations(latestSnapshotTimestampLabelValues)
	for _, combination := range latestSnapshotTimestampCombinations {
		LatestSnapshotTimestamp.With(prometheus.Labels(combination))
	}

	// SnapshotRequired
	snapshotRequiredLabelValues := map[string][]string{
		LabelKind: labels[LabelKind],
	}
	snapshotRequiredCombinations := generateLabelCombinations(snapshotRequiredLabelValues)
	for _, combination := range snapshotRequiredCombinations {
		SnapshotRequired.With(prometheus.Labels(combination))
	}

	// SnapshotDurationSeconds
	snapshotDurationSecondsLabelValues := map[string][]string{
		LabelKind:      labels[LabelKind],
		LabelSucceeded: labels[LabelSucceeded],
	}
	snapshotDurationSecondsCombinations := generateLabelCombinations(snapshotDurationSecondsLabelValues)
	for _, combination := range snapshotDurationSecondsCombinations {
		SnapshotDurationSeconds.With(prometheus.Labels(combination))
	}

	// ValidationDurationSeconds
	validationDurationSecondsLabelValues := map[string][]string{
		LabelSucceeded: labels[LabelSucceeded],
	}
	validationDurationSecondsCombinations := generateLabelCombinations(validationDurationSecondsLabelValues)
	for _, combination := range validationDurationSecondsCombinations {
		ValidationDurationSeconds.With(prometheus.Labels(combination))
	}

	// RestorationDurationSeconds
	restorationDurationSecondsLabelValues := map[string][]string{
		LabelSucceeded: labels[LabelSucceeded],
	}
	restorationDurationSecondsCombinations := generateLabelCombinations(restorationDurationSecondsLabelValues)
	for _, combination := range restorationDurationSecondsCombinations {
		RestorationDurationSeconds.With(prometheus.Labels(combination))
	}

	// DefragmentationDurationSeconds
	defragmentationDurationSecondsLabelValues := map[string][]string{
		LabelSucceeded: labels[LabelSucceeded],
	}
	defragmentationDurationSecondsCombinations := generateLabelCombinations(defragmentationDurationSecondsLabelValues)
	for _, combination := range defragmentationDurationSecondsCombinations {
		DefragmentationDurationSeconds.With(prometheus.Labels(combination))
	}

	// Metrics have to be registered to be exposed:
	prometheus.MustRegister(GCSnapshotCounter)

	prometheus.MustRegister(LatestSnapshotRevision)
	prometheus.MustRegister(LatestSnapshotTimestamp)
	prometheus.MustRegister(SnapshotRequired)

	prometheus.MustRegister(SnapshotDurationSeconds)
	prometheus.MustRegister(RestorationDurationSeconds)
	prometheus.MustRegister(ValidationDurationSeconds)
	prometheus.MustRegister(DefragmentationDurationSeconds)
}
