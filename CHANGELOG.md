# Changelog

## [Unreleased]

### Added

- Integration test for AWS

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

[Unreleased]: https://github.com/gardener/etcd-backup-restore/compare/0.2.3...HEAD
