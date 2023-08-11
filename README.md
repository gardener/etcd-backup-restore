# Etcd-Backup-Restore

[![CI Build status](https://concourse.ci.gardener.cloud/api/v1/teams/gardener/pipelines/etcd-backup-restore-master/jobs/master-head-update-job/badge)](https://concourse.ci.gardener.cloud/teams/gardener/pipelines/etcd-backup-restore-master/jobs/master-head-update-job)
[![Go Report Card](https://goreportcard.com/badge/github.com/gardener/etcd-backup-restore)](https://goreportcard.com/report/github.com/gardener/etcd-backup-restore)
[![GoDoc](https://godoc.org/github.com/gardener/etcd-backup-restore?status.svg)](https://godoc.org/github.com/gardener/etcd-backup-restore)

Etcd-backup-restore is collection of components to backup and restore the [etcd]. It also, provides the ability to validate the data directory, so that we could know the data directory is in good shape to bootstrap etcd successfully.

## Documentation Index

### Operations

* [Getting started](docs/deployment/getting_started.md)
* [Manual restoration](docs/operations/manual_restoration.md)
* [Monitoring](docs/operations/metrics.md)
* [Generating SSL certificates](docs/operations/generating_ssl_certificates.md)
* [Leader Election](docs/operations/leader_election.md)

### Design and Proposals

* [Core design](docs/proposals/design.md)
* [Etcd data validation](docs/proposals/validation.md)
* [Data restoration](docs/proposals/restoration.md)
* [High watch events ingress rate issue](docs/proposals/high_watch_event_ingress_rate.md)

### Development

* [Setting up a local development environment](docs/development/local_setup.md)
* [Testing and Dependency Management](docs/development/testing_and_dependencies.md)
* [Tests](docs/development/tests.md)
* [Adding support for a new object store provider](docs/development/new_cp_support.md)

[etcd]: https://github.com/etcd-io/etcd
