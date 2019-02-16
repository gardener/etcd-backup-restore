# Changelog

## 0.5.0

### Most notable changes

* *[USER]* Add new cloud provider OSS (Alibaba Object Storage Service) support for etcd-backup-restore (#108, @minchaow)
* *[USER]* Added configurable flag `delta-snapshot-memory-limit` to restrict memory consumption due to periodic delta snapshots. (#84, @swapnilgm)
* *[OPERATOR]* Fixed memory/goroutine leak: close previous Etcd watches (#116, @databus23)

### Improvements

* *[USER]* It now skips full snapshot if there were no updates on the etcd since previous full snapshot. (#86, @swapnilgm)
* *[USER]* Fixed the authentication call for swift to retry authentication on token expiration by setting `AllowReauth` flag for swift authentication call to `true`. (#80, @georgekuruvillak)
* *[OPERATOR]* Added the option to disable delta snapshots, by setting the 'delta-snapshot-period-seconds' flag to any value lesser than 1. (#96, @shreyas-s-rao)
* *[OPERATOR]* Added a sanity check to prevent data loss during initialization, by ensuring that the etcd revision is greater than or equal to the latest snapshot revision (#85, @shreyas-s-rao)
* *[OPERATOR]* Add mock test for GCS provider. (#82, @swapnilgm)
* *[OPERATOR]* There is now a helm chart to deploy etcd-backup-restore. (#59, @bergerx)

## 0.4.1

### Most notable changes

* *[USER]* Added configurable flag `delta-snapshot-memory-limit` to restrict memory consumption due to periodic delta snapshots. (#84, @swapnilgm)
* *[USER]* Fixed the authentication call for swift to retry authentication on token expiration by setting `AllowReauth` flag for swift authentication call to `true`. (#80, @georgekuruvillak)

## 0.4.0

### Most notable changes

* *[USER]* Now, snapshot upload happens in chunk. One can configure the number of parallel chunk uploads by setting command line argument `max-parallel-chunk-uploads`. Default is set to 5. (#68, @swapnilgm)
* *[OPERATOR]* Dynamic profiling support is added. Now we expose the `debug/pprof/*` endpoint to dynamically profile cpu, heap consumption. To enable profiling one has to explicitly set `enable-profiling`  on `server` sub-command. (#60, @swapnilgm)
* *[USER]* At the time of restoration, the etcd data directory will be restored to temporary directory with suffix `.part` i.e.`<path-to-etcd-data-dir>.part`. On successful restoration we will replace actual etcd data directory with this. This brings standard and more cleaner approach to restoration. (#58, @georgekuruvillak)
* *[USER]* Restoration time optimised by parallelising the fetching of delta snapshots. Added the --max-fetchers flag to the etcdbrctl command to specify the maximum number of fetcher threads that are allowed to run in parallel. (#57, @shreyas-s-rao)
* *[USER]* Etcd-backup-restore utility when started in server mode will start defragmenting etcd member data directory periodically, so that db size will be under control. You can set defragmentation period in number of hours by setting `defragmentation-period-in-hours` flag on `etcdbrctl server` command. (#55, @swapnilgm)

### Improvements

* *[OPERATOR]* Updated the azure sdk as per the recommendation in [doc](https://github.com/Azure/azure-sdk-for-go/tree/master/storage#azure-storage-sdk-for-go-preview) to use  package `github.com/Azure/azure-storage-blob-go` as azure storage SDK, (#76, @swapnilgm)
* *[OPERATOR]* Added mock test for Openstack Swift snapstore. (#75, @swapnilgm)
* *[OPERATOR]* Snapshot are now uploaded in chunks, considering only erroneous chunk upload in case of failure. (#53, @swapnilgm)

## 0.3.1

### Fixed

- Previously, in case of a failed delta snapshot, data directory was marked as valid even though there is data loss due to further delta snapshots in queue which weren't getting applied.

### Added

- Integration test for AWS:
  - Covers restoration, data directory validation and snapshotter code.

## 0.3.0 - 2018-06-26

Docker Image: __eu.gcr.io/gardener-project/gardener/etcdbrctl:0.3.0__

### Added

- Incremental Backup-restore (https://github.com/gardener/etcd-backup-restore/pull/29):
  - In backup, full snapshot is taken first and then we apply watch and persist the logs accumulated over certain period to snapshot store. 
  - Restore process, restores from the full snapshot, start the embedded etcd and apply the logged events one by one.
  - Checksum is append in delta snapshot for verification.

- Exponential garbage collection (https://github.com/gardener/etcd-backup-restore/pull/31):
  - Introduces new argument: 
  - Keep only the last 24 hourly backups and of all other backups only the last backup in a day
  - Keep only the last 7 daily backups and of all other backups only the last backup in a week
  - Keep only the last 4 weekly backups
  - Unit tests are added for same
 
- Initial setup for Integration test for AWS

## 0.2.3 - 2018-05-22

Docker Image: __eu.gcr.io/gardener-project/gardener/etcdbrctl:0.2.3__

### Fixed

- Etcd made unready in case backup container fails to take periodic backups.
- Backup container made to retry exponentially rather than crash out.

### Changed

- Readiness probe renamed from `/metrics` to `/healthz`.

## 0.2.2 - 2018-04-30

Docker Image: __eu.gcr.io/gardener-project/gardener/etcdbrctl:0.2.2__ 

### Added

- TLS support for etcd endpoints.

### Changed

- Delete contents of data directory instead of the directory.

## 0.1.0 - 2018-04-09

Docker Image: __eu.gcr.io/gardener-project/gardener/etcdbrctl:0.1.0__

### Added

- Take snapshot of etcd at periodic interval.
- Save snapshot to object stores provided by AWS, Azure, GCS, Openstack and also to local disk.
- Verify data directory of etcd for corruption before bootstrapping.
- Restore the etcd data directory from previous snapshot. 

[Unreleased]: https://github.com/gardener/etcd-backup-restore/compare/0.4.1...HEAD
