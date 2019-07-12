# Getting started

Currently we don't publish the binary build with the release, but it is pretty straight forward to build it by following the steps mentioned [here](../development/local_setup.md#build). But we do publish the docker image with each release, please check the [release page](https://github.com/gardener/etcd-backup-restore/releases) for the same.

## Usage

You can follow the `help` flag on `etcdbrctl` command and its sub-commands to know the usage details. Some of the common use cases are mentioned below. Although examples below uses AWS S3 as storage provider, we have added support for AWS, GCS, Azure, Openstack swift and Alicloud OSS object store. It also supports local disk as storage provider.

### Cloud Provider Credentials

The procedure to provide credentials to access the cloud provider object store varies for different providers.

* For `AWS S3`, the `credentials` file has to be provided in the `~/.aws` directory.

* For `GCP Containers`, the service account json file should be provided in the `~/.gcp` as a `service-account-file.json` file.

* For `Azure Blob storage`, `STORAGE_ACCOUNT` and `STORAGE_KEY` should be made available as environment variables.

* For `Openstack Swift`, `OS_USERNAME`, `OS_PASSWORD`, `OS_AUTH_URL`, `OS_TENANT_ID` and `OS_DOMAIN_ID` should be made available as environment variables.

* For `Alicloud OSS`, `ALICLOUD_ENDPOINT`, `ALICLOUD_ACCESS_KEY_ID`, `ALICLOUD_ACCESS_KEY_SECRET` should be made available as environment variables.

### Taking scheduled snapshot

`etcd` should already be running. One can apply standard cron format scheduling for regular backup of etcd. The cron schedule is used to take full backups. The delta snapshots are taken at regular intervals in the period in between full snapshots as indicated by the `delta-snapshot-period-seconds` flag. The default for the same is 10 seconds.

etcd-backup-restore has two garbage collection policies to collect existing backups from the cloud bucket. The flag `garbage-collection-policy` is used to indicate the correct garbage collection policy.
1. `Exponential`
1. `LimitBased`

If using `LimitBased` policy, the `max-backups` flag should be provided to indicate the number of recent backups to persist at each garbage collection cycle.

```console
$ ./bin/etcdbrctl snapshot --storage-provider="S3" --etcd-endpoints http://localhost:2379 --schedule "*/1 * * * *" --store-container="etcd-backup" --delta-snapshot-period-seconds=10 --max-backups=10 --garbage-collection-policy='LimitBased'
INFO[0000] Validating schedule...
INFO[0000] Job attempt: 1
INFO[0000] Taking initial full snapshot at time: 2018-07-09 12:09:04.3567024 +0000 UTC
INFO[0000] Successfully opened snapshot reader on etcd
INFO[0000] Successfully saved full snapshot at: Backup-1531138145/Full-00000000-00000001-1531138145
INFO[0000] Will take next full snapshot at time: 2018-07-09 12:10:00 +0000 UTC
INFO[0000] Applied watch on etcd from revision: 00000002
INFO[0000] No events received to save snapshot.
```

The command mentioned above takes hourly snapshots and pushs it to S3 bucket named "etcd-backup". It is configured to keep only last 10 backups in bucket.

`Exponential` policy stores the snapshots in a condensed manner as mentioned below:
- All full backups and delta backups for the previous hour.
- Latest full snapshot of each previous hour for the day.
- Latest full snapshot of each previous day for 7 days.
- Latest full snapshot of the previous 4 weeks.

```console
$ ./bin/etcdbrctl snapshot --storage-provider="S3" --etcd-endpoints http://localhost:2379 --schedule "*/1 * * * *" --store-container="etcd-backup" --delta-snapshot-period-seconds=10 --garbage-collection-policy='Exponential'
INFO[0000] Validating schedule...
INFO[0000] Job attempt: 1
INFO[0000] Taking initial full snapshot at time: 2018-07-09 12:09:04.3567024 +0000 UTC
INFO[0000] Successfully opened snapshot reader on etcd
INFO[0000] Successfully saved full snapshot at: Backup-1531138145/Full-00000000-00000001-1531138145
INFO[0000] Will take next full snapshot at time: 2018-07-09 12:10:00 +0000 UTC
INFO[0000] Applied watch on etcd from revision: 00000002
INFO[0000] No events received to save snapshot.
```

The command mentioned above stores etcd snapshots as per the exponential policy mentioned above.

### Etcd data directory initialization

Sub-command `initialize` does the task of data directory validation. If the data directory is found to be corrupt, the controller will restore it from the latest snapshot in the cloud store. It restores the full snapshot first and then incrementally applies the delta snapshots.

```console
$ ./bin/etcdbrctl initialize --storage-provider="S3" --store-container="etcd-backup" --data-dir="default.etcd"
INFO[0000] Checking for data directory structure validity...
INFO[0000] Checking for data directory files corruption...
INFO[0000] Verifying snap directory...
Verifying Snapfile default.etcd/member/snap/0000000000000001-0000000000000001.snap.
INFO[0000] Verifying WAL directory...
INFO[0000] Verifying DB file...
INFO[0000] Data directory corrupt. Invalid db files: invalid database
INFO[0000] Removing data directory(default.etcd) for snapshot restoration.
INFO[0000] Finding latest snapshot...
INFO[0000] Restoring from latest snapshot: Full-00000000-00040010-1522152360...
2018-03-27 17:38:06.617280 I | etcdserver/membership: added member 8e9e05c52164694d [http://localhost:2380] to cluster cdf818194e3a8c32
INFO[0000] Successfully restored the etcd data directory.
```

### Etcdbrctl server

With sub-command `server` you can start a http server which exposes an endpoint to initialize etcd over REST interface. The server also keeps on backup schedule thread running to have periodic backups. This is mainly made available to manage an etcd instance running in a Kubernetes cluster. You can deploy the example [helm chart](../../chart/etcd-backup-restore) on a Kubernetes cluster to have an fault resilient etcd instance.
