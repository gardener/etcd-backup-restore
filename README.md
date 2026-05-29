# Etcd-Backup-Restore
[![REUSE status](https://api.reuse.software/badge/github.com/gardener/etcd-backup-restore)](https://api.reuse.software/info/github.com/gardener/etcd-backup-restore)
[![Go Report Card](https://goreportcard.com/badge/github.com/gardener/etcd-backup-restore)](https://goreportcard.com/report/github.com/gardener/etcd-backup-restore)
[![GoDoc](https://godoc.org/github.com/gardener/etcd-backup-restore?status.svg)](https://godoc.org/github.com/gardener/etcd-backup-restore)

Etcd-backup-restore is collection of components for backuping and restoring the [etcd](https://github.com/etcd-io/etcd/). It also validates the etcd's data directory and auto-triggers restoration when needed.

etcd-backup-restore currently provides the following capabilities (the list is not comprehensive):

  - **Backups**: [etcd](https://github.com/etcd-io/etcd) database snapshots(a.k.a full snapshots) and incremental snapshots are taken regularly, compressed and stored in the configured object storage provider.
  - **Data directory validation**: It does [etcd](https://github.com/etcd-io/etcd) data-directory validation before starting any etcd member.
  - **Restoration**: In case of a database corruption for a single-member cluster it restores from latest set of snapshots (full & delta).
  - **Zero-downtime member recovery**: Replaces a failed member in a multi-member etcd cluster without any downtime.
  - **Automated maintenance**: Act as watchDog to make etcd cluster lean and clean by running [maintenance](https://etcd.io/docs/v3.5/op-guide/maintenance/) operations such as defragmentation on a configured schedule.
  - **Secondary backups**: Uploading the backups to secondary object store (if configured).
  - **Metrics**: Exposes metrics to monitor the health and status of etcd-backup-restore operations.

For detailed documentation, see our `/docs` folder. Please find the [index](docs/README.md) here.

## Contributions

If you wish to contribute then please see our [contributor guidelines](development/contribution.md).

## Feedback and Support

We always look forward to active community engagement. Please report bugs or suggestions on how we can enhance `etcd-backup-restore` on [GitHub Issues](https://github.com/gardener/etcd-backup-restore/issues).

## License

Release under [Apache-2.0](https://github.com/gardener/etcd-backup-restore/blob/master/LICENSE) license.
