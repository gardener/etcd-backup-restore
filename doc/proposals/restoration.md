# Etcd data restoration

Etcd-backup-restore provides a simple and automated method of restoring single-node etcd clusters from their backups (snapshots) stored via the snapshotting process. Restoration kicks in when etcd data gets corrupted, either by the etcd process itself or due to PV corruption, or if the data is manipulated or erased due to human intervention. The restorer relies on the backups stored in an object storage bucket to restore the etcd data to its latest backed-up non-corrupt state.

In the context of etcd-backup-restore server, restoration is a conditional part of the initialization workflow, in which restoration is triggered when etcd data validation deems the data directory invalid or corrupt.

To learn about starting an etcd-backup-restore server, please follow the [getting started guide](../usage/getting_started.md).

## Assumption

- Data directory must not contain `member` directory before triggering restoration
- Snapshotter should NOT be running
  - This is taken care of by etcd-backup-restore when started in `server` mode
  - If running in `snapshot` mode, one must take precautions to stop the snapshotter before attempting to restore

## Requirements

- Etcd backup stored in an object storage bucket like AWS S3, GCP GCS, Azure ABS, Openstack Swift or Alicloud OSS
- The backup format must conform to that of etcd-backup-restore's snapshotter
  - Each backup must have a full snapshot, optionally followed by a set of delta/incremental snapshots

## Workflow

![workflow](../images/restorer-sequence-diagram.png)

- Full snapshot is essentially the etcd DB file encapsulated in a JSON object
- The full snapshot is downloaded only if available. If not, the embedded etcd server takes care of initializing the DB file
- Delta snapshots are downloaded and applied only if available. If not, this step is skipped and restoration is marked as complete.

Restorer fetches the latest full snapshot present in the snapstore, named `etcd-main/v1/Backup-xxxxxxxxxx/Full-aaaaaaaa-bbbbbbbb-xxxxxxxxxx`, and restores from it. It then fetches the subsequent delta snapshots and applies them sequentially on top of the partially restored data directory. These delta snapshots are named `etcd-main/v1/Backup-xxxxxxxxxx/Incr-bbbbbbbb-cccccccc-yyyyyyyyyy` and so on. Please consider the following while reading the aforementioned snapshot naming patterns: `xxxxxxxxxx`,`yyyyyyyyyy` are UTC timestamps and `aaaaaaaa`,`bbbbbbbb`,`cccccccc` are etcd revision numbers. Each snapshot contains etcd events between two revisions inclusive, as specified in the snapshot name.

## Automatic restoration

Restoration is an essential part of etcd-backup-restore's `initialization` workflow, which is triggered as the first step in the `server` sub-command workflow. Ideally, operators need not worry about restoration when deploying etcd-backup-restore via the provided helm chart, where the tool is deployed in the `server` mode, as restoration will be triggered if the etcd data directory is found to be corrupt.

## Manual restoration

Please refer to [this guide](../usage/manual_restoration.md) to know how to manually restore the etcd data, the caveats that come with it, and how to avoid common pitfalls.
