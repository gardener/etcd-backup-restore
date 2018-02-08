# Etcd backup restore design

## Goal

Main goal of this project to provide a solution to make [etcd] instance backing the kubernetes cluster robust to failures. Etcd data is backed up at regular intervals. The etcd instance runs on a Seed Kubernetes cluster and stores the state of the Shoot Kubernetes clusters. In case of etcd instance failures, the etcd instance is reconciled and in the extreme case restored from the latest non-corrupt backup available.

## Non Goal

- Volume failures like EBS volume failures, PVC and PV deletion after etcd deletion (when the shoot goes down) are not handled at this moment. However, checks and remedial steps for the failure types mentioned previously will be implemented over time/later.
- Allowing the user to pick from a list of backups or even point-in-time-recovery (PiTR) is not planned, because there is no reliable infrastructure reconciliation implemented in Kubernetes and the CRD extension concept (or API server extensions) allows to have resources with impact on the external/physical world that would not be reconciled either (if we are to restore a backup which is not the latest).
- Backup validation is not repeatedly done, i.e. once a backup is taken, we assume the infrastructure preserves it in a healthy state (we do not validate backups by rehashing them and comparing them with initially taken hashes).

## Design

### Assumption

- Etcd cluster is a single member cluster (Shoot cluster environment, much like a master in a Borg-hosted GKE cluster).

- Etcd instance will be deployed as a StatefulSet with a PVC for its persistence, which will bring free reconciliation.

### Requirements

- ETCD pod fails and PVC with valid data directory is available: Etcd pod should restart and attach to same PVC to continue from last state before failure.

- Data corruption check: There should be mechanism to check etcd failure due to etcd data directory corruption.

- Data directory unavailable but backups available in the cloud store: Restore Etcd from the latest backup.

- Backup etcd snapshots on different cloud object-stores.

### Rely on K8s for the Following

- PVC deleted - Doesn't necessarily detach volume if in use.
- IaaS failure
  - Storage Volume detached: PV reattaches to the same volume. Restoration of Etcd data directory not required.
  - Storage Volume deleted: Expect StatefulSet to be robust to such failures. Restoration of etcd data directory necessary.
  - Node failure: K8s should reschedule the pod on a different node. Restoration of Etcd data directory not required.
  - ETCD Pod scheduling failed: Can’t do much (look pod configuration parameter for critical-pods).

## Architecture

![architecture](images/etcd-backup-restore.jpg)

The architecture diagram is pretty self explanatory. We will have StatefulSet for etcd with two init containers in it. Also, we will have backup sidecar to take backups at regular intervals.

### Container Responsibilities

#### Init Container

- Check if data directory exists. Check for data corruption. If data directory is in corrupted state, clear the data directory.
- Check if data directory exists. If not, try to perform an Etcd data restoration from the latest snapshot.
- If no data directory is present and no snapshots are available, start etcd as a fresh installation.

#### Backup Sidecar (preferred via container)

- Takes snapshot from the running etcd instance.
- Schedule the backup operation (probably using cron library) which triggers full snapshot at regular intervals.
- Store the snapshot in the configured cloud object store.

### Handling of Different Scenarios/Issues

- DNS latency: Should not matter for single member Etcd cluster.
- Etcd upgrade and downgrade for K8s compatibility: Should not be issue for v3.* series released so far. Simply restart pod. No data format change.
- Iaas issue: Issues like unreachable object store, will be taken care by init container and backup container. Both container will keep retrying to reach out object store with exponential timeouts.
- Corrupt backup: StatefulSet go in restart loop, and human operator will with customers concern delete the last corrupt backup from object store manually. So that, in next iteration it will recover from previous non-corrupt backup.

## Outlook

We want to develop incremental/continuous etcd backups (write watch logs in between full backups), to ensure our backups are fresh enough to avoid discrepancies between etcd backup and external/physical world.

[etcd]: https://github.com/coreos/etcd