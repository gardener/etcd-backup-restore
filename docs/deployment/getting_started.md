# Getting started

The binary builds are not published with each release, but it is pretty straight forward to build it by following the steps mentioned [here](../development/local_setup.md#build). But we do publish the docker image with each release, please check the [release page](https://github.com/gardener/etcd-backup-restore/releases) for the same. Currently, release docker images are pushed to `europe-docker.pkg.dev/gardener-project/public/gardener/etcdbrctl` container  repository.

## Usage

You can follow the `help` flag on `etcdbrctl` command and its sub-commands to know the usage details. Some common use cases are mentioned below. Although examples below use `AWS S3` as storage provider, etcd-backup-restore supports AWS S3, GCS, Azure Blob Storage, OpenStack Swift, and AliCloud OSS object store. It also supports local disk as storage provider for development purposes, but it is not recommended to use this in a production environment.

### Cloud Provider Credentials

The procedure to provide credentials to access the cloud provider object store varies for different providers, the method to pass credentials for each provider is [described below](#passing-credentials).

### Passing Credentials

* For `AWS S3`:
   1. The secret file should be provided, and the file path should be made available as an environment variable: `AWS_APPLICATION_CREDENTIALS`. AWS S3 supports two alternative authentication options:
      1. Static credentials: `accessKeyID` and `secretAccessKey` must be set.
      1. Web Identity Role: `roleARN` and `tokenPath` must be set.
   2. For `S3-compatible providers` such as MinIO, `s3ForcePathStyle`, `insecureSkipVerify`, `trustedCaCert`, `requestChecksumCalculation` and `responseChecksumValidation`, can also be made available in an above file to configure the S3 client to communicate to a non-AWS provider.
   3. To enable Server-Side Encryption using Customer Managed Keys for `S3-compatible providers`, use `sseCustomerKey` and `sseCustomerAlgorithm` in the credentials file above. For example, `sseCustomerAlgorithm` could be set to `AES256`, and correspondingly the `sseCustomerKey` is set to a valid AES-256 key.
   4. To override the endpoint of the S3 compatible API, use the `--store-endpoint-override` flag and pass the corresponding URL. This can be used for S3 compatible providers like Ceph, MinIO, etc., or while using the localstack S3 emulator.

* For `Google Cloud Storage`:
   1. GCS supports two alternative authentication options:
      1. Static credentials, they should be provided via a file in the `~/.gcp` folder named `service-account-file.json`.This file must have field named `type` with value `service_account`.
      1. Workload Identity credentials, they should be provided via a file in the `~/.gcp` folder named `credentialsConfig` which have field named `type` with value `external_account`. Additionally, it is expected in the `~/.gcp` folder files named `projectID` and `token` to be provided, they contain the GCP project ID and the JWT respectively.
   2. The service account json or the credentials config file should be provided, and the path to it should be made available as environment variable `GOOGLE_APPLICATION_CREDENTIALS`.
   3. If using a storage API [endpoint override](https://pkg.go.dev/cloud.google.com/go#hdr-Endpoint_Override), such as a [regional endpoint](https://cloud.google.com/storage/docs/regional-endpoints) or a local GCS emulator endpoint, the endpoint must be passed through the `--store-endpoint-override` flag.

* For `Azure Blob storage`:
   1. The JSON secret file should be provided, and the file path should be made available as an environment variable: `AZURE_APPLICATION_CREDENTIALS`.
   2. The Azure Blob Storage domain can be overridden by passing the overriding endpoint through the `--store-endpoint-override` flag. This is to be used if the endpoint of your Azure API is not the default (like Azure China or Azure Government), or while using the Azurite emulator.

* For `Openstack Swift`:
  1. The secret file should be provided, and the file path should be made available as an environment variable: `OPENSTACK_APPLICATION_CREDENTIALS`.

* For `Alicloud OSS`:
  1. The secret file should be provided, and the file path should be made available as an environment variable: `ALICLOUD_APPLICATION_CREDENTIALS`.

* For `Dell EMC ECS`:
  1. `ECS_ENDPOINT`, `ECS_ACCESS_KEY_ID`, `ECS_SECRET_ACCESS_KEY` should be made available as environment variables. For development purposes, the environment variables `ECS_DISABLE_SSL` and `ECS_INSECURE_SKIP_VERIFY` can also be set to "true" or "false".

* For `Openshift Container Storage (OCS)`:
  1. The secret file should be provided, and the file path should be made available as an environment variable: `OPENSHIFT_APPLICATION_CREDENTIALS`.
  For development purposes, the environment variables `OCS_DISABLE_SSL` and `OCS_INSECURE_SKIP_VERIFY` can also be set to "true" or "false".

Check the [example of storage provider secrets](https://github.com/gardener/etcd-backup-restore/tree/master/example/storage-provider-secrets)

### Taking scheduled snapshot

Sub-command `snapshot` takes scheduled backups, or `snapshots` of a running `etcd` cluster, which are pushed to one of the storage providers specified above (please note that `etcd` should already be running). One can apply standard Cron format scheduling for regular backup of etcd. The Cron schedule is used to take full backups. The delta snapshots are taken at regular intervals in the period in between full snapshots as indicated by the `delta-snapshot-period` flag. The default for the same is 20 seconds.

etcd-backup-restore has two garbage collection policies to clean up existing backups from the cloud bucket. The flag `garbage-collection-policy` is used to indicate the desired garbage collection policy.

1. `Exponential`
1. `LimitBased`

If using `LimitBased` policy, the `max-backups` flag should be provided to indicate the number of recent-most backups to persist at each garbage collection cycle.

```console
$ ./bin/etcdbrctl snapshot  \
--storage-provider="S3" \
--endpoints http://localhost:2379 \
--schedule "*/1 * * * *" \
--store-container="etcd-backup" \
--delta-snapshot-period=10s \
--max-backups=10 \
--garbage-collection-policy='LimitBased'

INFO[0000] etcd-backup-restore Version: 0.7.0-dev
INFO[0000] Git SHA: c03f75c
INFO[0000] Go Version: go1.12.7
INFO[0000] Go OS/Arch: darwin/amd64
INFO[0000] Validating schedule...
INFO[0000] Defragmentation period :72 hours
INFO[0000] Taking scheduled snapshot for time: 2019-08-05 21:41:34.303439 +0530 IST
INFO[0000] Successfully opened snapshot reader on etcd
INFO[0001] Successfully initiated the multipart upload with upload ID : xhDeLNQsp9HAExmU1O4C3mCriUViVIRrrlPzdJ_.f4dtL046pNekEz54UD9GLYYOLjQUy.ZLZBLp4WeyNnFndDbvDZwhhCjAtwZQdqEbGw5.0HnX8fiP9Vvqk3_2j_Cf
INFO[0001] Uploading snapshot of size: 22028320, chunkSize: 5242880, noOfChunks: 5
INFO[0001] Triggered chunk upload for all chunks, total: 5
INFO[0001] No of Chunks:= 5
INFO[0001] Uploading chunk with id: 2, offset: 5242880, attempt: 0
INFO[0001] Uploading chunk with id: 4, offset: 15728640, attempt: 0
INFO[0001] Uploading chunk with id: 5, offset: 20971520, attempt: 0
INFO[0001] Uploading chunk with id: 1, offset: 0, attempt: 0
INFO[0001] Uploading chunk with id: 3, offset: 10485760, attempt: 0
INFO[0008] Received chunk result for id: 5, offset: 20971520
INFO[0012] Received chunk result for id: 3, offset: 10485760
INFO[0014] Received chunk result for id: 4, offset: 15728640
INFO[0015] Received chunk result for id: 2, offset: 5242880
INFO[0018] Received chunk result for id: 1, offset: 0
INFO[0018] Received successful chunk result for all chunks. Stopping workers.
INFO[0018] Finishing the multipart upload with upload ID : xhDeLNQsp9HAExmU1O4C3mCriUViVIRrrlPzdJ_.f4dtL046pNekEz54UD9GLYYOLjQUy.ZLZBLp4WeyNnFndDbvDZwhhCjAtwZQdqEbGw5.0HnX8fiP9Vvqk3_2j_Cf
INFO[0018] Total time to save snapshot: 17.934609 seconds.
INFO[0018] Successfully saved full snapshot at: Backup-1565021494/Full-00000000-00009002-1565021494
INFO[0018] Applied watch on etcd from revision: 9003
INFO[0018] Stopping full snapshot...
INFO[0018] Resetting full snapshot to run after 7.742179s
INFO[0018] Will take next full snapshot at time: 2019-08-05 21:42:00 +0530 IST
INFO[0018] Taking delta snapshot for time: 2019-08-05 21:41:52.258109 +0530 IST
INFO[0018] No events received to save snapshot. Skipping delta snapshot.
```

The command mentioned above takes hourly snapshots and push it to S3 bucket named "etcd-backup". It is configured to keep only last 10 backups in bucket.

`Exponential` policy stores the snapshots in a condensed manner as mentioned below:
- All full backups and delta backups for the previous hour.
- Latest full snapshot of each previous hour for the day.
- Latest full snapshot of each previous day for 7 days.
- Latest full snapshot of the previous 4 weeks.

```console
$ ./bin/etcdbrctl snapshot \
--storage-provider="S3" \
--endpoints http://localhost:2379 \
--schedule "*/1 * * * *" \
--store-container="etcd-backup" \
--delta-snapshot-period=10s \
--garbage-collection-policy='Exponential'

INFO[0000] etcd-backup-restore Version: 0.7.0-dev
INFO[0000] Git SHA: c03f75c
INFO[0000] Go Version: go1.12.7
INFO[0000] Go OS/Arch: darwin/amd64
INFO[0000] Validating schedule...
INFO[0001] Taking scheduled snapshot for time: 2019-08-05 21:50:07.390127 +0530 IST
INFO[0001] Defragmentation period :72 hours
INFO[0001] There are no updates since last snapshot, skipping full snapshot.
INFO[0001] Applied watch on etcd from revision: 9003
INFO[0001] Stopping full snapshot...
INFO[0001] Resetting full snapshot to run after 52.597795s
INFO[0001] Will take next full snapshot at time: 2019-08-05 21:51:00 +0530 IST
INFO[0001] Taking delta snapshot for time: 2019-08-05 21:50:07.402289 +0530 IST
INFO[0001] No events received to save snapshot. Skipping delta snapshot.
INFO[0001] Stopping delta snapshot...
INFO[0001] Resetting delta snapshot to run after 10 secs.
INFO[0011] Taking delta snapshot for time: 2019-08-05 21:50:17.403706 +0530 IST
INFO[0011] No events received to save snapshot. Skipping delta snapshot.
INFO[0011] Stopping delta snapshot...
INFO[0011] Resetting delta snapshot to run after 10 secs.
INFO[0021] Taking delta snapshot for time: 2019-08-05 21:50:27.406208 +0530 IST
INFO[0021] No events received to save snapshot. Skipping delta snapshot.
```

The command mentioned above stores etcd snapshots as per the exponential policy mentioned above.

### Etcd data directory initialization

Sub-command `initialize` does the task of data directory validation. If the data directory is found to be corrupt, the controller will restore it from the latest snapshot in the cloud store. It restores the full snapshot first and then incrementally applies the delta snapshots. For more information regarding data restoration, please refer to [this guide](../proposals/restoration.md).

```console
$ ./bin/etcdbrctl initialize \
--storage-provider="S3" \
--store-container="etcd-backup" \
--data-dir="default.etcd"
INFO[0000] Checking for data directory structure validity...
INFO[0000] Checking for revision consistency...
INFO[0000] Etcd revision inconsistent with latest snapshot revision: current etcd revision (770) is less than latest snapshot revision (9002): possible data loss
INFO[0000] Finding latest set of snapshot to recover from...
INFO[0001] Removing data directory(default.etcd.part) for snapshot restoration.
INFO[0001] Restoring from base snapshot: Backup-1565021494/Full-00000000-00009002-1565021494
2019-08-05 21:45:49.646232 I | etcdserver/membership: added member 8e9e05c52164694d [http://localhost:2380] to cluster cdf818194e3a8c32
INFO[0008] No delta snapshots present over base snapshot.
INFO[0008] Removing data directory(default.etcd) for snapshot restoration.
INFO[0008] Successfully restored the etcd data directory.
```

### Etcdbrctl server

With sub-command `server` you can start a http server which exposes an endpoint to initialize etcd over REST interface. The server also keeps the backup schedule thread running to keep taking periodic backups. This is mainly made available to manage an etcd instance running in a Kubernetes cluster. You can deploy the example [helm chart](../../chart/etcd-backup-restore) on a Kubernetes cluster to have a fault-resilient, self-healing etcd cluster.

> [!NOTE]
> When deployed with the helm chart, only the static single member & static multi-member etcd cluster configurations are supported. The dynamic etcd cluster configuration is not supported. That is 0 to 1 or 0 to 3 member clusters are supported but not 1 to 3 member clusters. This is due to extra complexity in handling the scale-up scenario which cannot be brought into the helm charts at the moment. We recommend using [etcd-druid](https://github.com/gardener/etcd-druid/) for full-fledged etcd cluster management.

## Etcdbrctl copy

With sub-command `copy` you can copy all snapshots (Full and Delta) fom one snapstore to another. Using the two filter parameters `max-backups-to-copy` and `max-backup-age` you can also limit the number of snapshots that will be copied or target only the newest snapshots.

```console
$ ./bin/etcdbrctl copy \
--storage-provider="GCS" \
--snapstore-temp-directory="/temp" \
--store-prefix="target-prefix" \
--store-container="target-container" \
--source-store-prefix="prefix" \
--source-store-container="container" \
--source-storage-provider="GCS" \
--max-backup-age=15 \
INFO[0000] etcd-backup-restore Version: v0.14.0-dev
INFO[0000] Git SHA: b821ee55
INFO[0000] Go Version: go1.16.5
INFO[0000] Go OS/Arch: darwin/amd64
INFO[0000] Getting source backups...                     actor=copier
...
INFO[0026] Copying Incr snapshot Incr-ID.gz...  actor=copier
INFO[0027] Uploading snapshot of size: 123456, chunkSize: 123456, noOfChunks: 1
INFO[0027] Triggered chunk upload for all chunks, total: 1
INFO[0027] No of Chunks:= 1
INFO[0027] Uploading chunk with offset : 0, attempt: 0
INFO[0027] Received chunk result for id: 1, offset: 0
INFO[0027] Received successful chunk result for all chunks. Stopping workers.
INFO[0027] All chunk uploaded successfully. Uploading composite object.
INFO[0027] Composite object uploaded successfully.
INFO[0027] Shutting down...
```
