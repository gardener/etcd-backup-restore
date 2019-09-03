# Monitoring

etcd-backup-restore uses [Prometheus][prometheus] for metrics reporting. The metrics can be used for real-time monitoring and debugging. It won't persist its metrics; if a member restarts, the metrics will be reset.

The simplest way to see the available metrics is to cURL the metrics endpoint `/metrics`. The format is described [here](http://prometheus.io/docs/instrumenting/exposition_formats/).

Follow the [Prometheus getting started doc][prometheus-getting-started] to spin up a Prometheus server to collect etcd metrics.

The naming of metrics follows the suggested [Prometheus best practices][prometheus-naming]. All etcd-backup-restore related metrics are put under namespace `etcdbr`.

## ETCD metrics

The metrics under the `etcd` prefix/namespace are carried forward from etcd library that we use. These metrics do not include details of the `etcd` deployment on which `etcd-backup-restore` utility operates. Instead, it helps in monitoring the `embedded etcd` we spawn during restoration process.

### Snapshot

These metrics describe the status of the snapshotter. In order to detect outages or problems for troubleshooting, these metrics should be closely monitored. The below mentioned metrics are listed as collection of series using prometheus labels `kind` and `succeeded`. `Kind` label indicates the snapshot kind i.e. full snapshot or incremental/delta snapshot in the context. And succeeded indicates whether the metrics is for successful operation or erroneous operation.

| Name | Description | Type |
|------|-------------|------|
| etcdbr_snapshot_duration_seconds | Total latency distribution of saving snapshot to object store. | Histogram |
| etcdbr_snapshot_gc_total | Total number of garbage collected snapshots. | Counter |
| etcdbr_snapshot_latest_revision | Revision number of latest snapshot taken. | Gauge |
| etcdbr_snapshot_latest_timestamp | Timestamp of latest snapshot taken. | Gauge |
| etcdbr_snapshot_required | Indicates whether a new snapshot is required to be taken. | Gauge |

Abnormally high snapshot duration (`etcdbr_snapshot_duration_seconds`) indicates disk issues and low network bandwidth.

`etcdbr_snapshot_latest_timestamp` indicates the time when last snapshot was taken. If it has been a long time since a snapshot has been taken, then it indicates either the snapshots are being skipped because of no updates on etcd or :warning: something fishy is going on and a possible data loss might occur on the next restoration.

`etcdbr_snapshot_gc_total` gives the total number of snapshots garbage collected since bootstrap. You can use this in coordination with `etcdbr_snapshot_duration_seconds_count` to get number of snapshots in object store.

`etcdbr_snapshot_required` indicates whether a new snapshot is required to be taken. Acts as a boolean flag where zero value implies 'false' and non-zero values imply 'true'. :warning: This metric does not work as expected for the case where delta snapshots are disabled (by setting the etcdbrctl flag `delta-snapshot-period-seconds` to 0).

### Defragmentation

The metrics for defragmentation is of type histogram, which gives the number of times defragmentation was triggered. :warning: The defragmentation latency should be as low as possible, since
defragmentation is indeed a costly operation, which results in unavailability of etcd for the period.

| Name | Description | Type |
|------|-------------|------|
| etcdbr_defragmentation_duration_seconds | Total latency distribution of defragmentation of etcd data directory. | Histogram |

### Validation and Restoration

Two major steps in initialization of etcd data directory are validation and restoration. It is necessary to monitor the count and time duration of these calls, from a high availability perspective.

| Name | Description | Type |
|------|-------------|------|
| etcdbr_validation_duration_seconds | Total latency distribution of validating data directory. | Histogram |
| etcdbr_restoration_duration_seconds | Total latency distribution of restoring from snapshot. | Histogram |

### Network

These metrics describe the status of the network usage. We use `/proc/<etcdbr-pid>/net/dev` to get network usage details for the etcdbr process. Currently these metrics are only supported on linux-based distributions.

All these metrics are under subsystem `network`.

| Name | Description | Type |
|------|-------------|------|
| etcdbr_network_transmitted_bytes | The total number of bytes received over network. | Counter |
| etcdbr_network_received_bytes | The total number of bytes received over network. | Counter |

`etcdbr_network_transmitted_bytes` counts the total number of bytes transmitted. Usually this reflects the data uploaded to object store as part of snapshot uploads.

`etcdbr_network_received_bytes` counts the total number of bytes received. Usually this reflects the data received as part of snapshots from actual `etcd`. There could be a sudden spike in this at the time of restoration as well.

## gRPC requests

These metrics are exposed via [go-grpc-prometheus][go-grpc-prometheus].

## Prometheus supplied metrics

The Prometheus client library provides a number of metrics under the `go` and `process` namespaces.

[glossary-proposal]: learning/glossary.md#proposal
[prometheus]: http://prometheus.io/
[prometheus-getting-started]: http://prometheus.io/docs/introduction/getting_started/
[prometheus-naming]: http://prometheus.io/docs/practices/naming/
[v2-http-metrics]: v2/metrics.md#http-requests
[go-grpc-prometheus]: https://github.com/grpc-ecosystem/go-grpc-prometheus
