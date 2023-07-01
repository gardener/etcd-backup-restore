# Tests

`etcd-backup-restore` makes use of three sets of tests - unit tests in each package, integration tests to ensure the working of the overall tool and performance regression tests to check changes in resource consumption between different versions of etcd-backup-restore.

### Integration tests

Integration tests include the basic working of:

- **snapshotting**: successfully upload full and delta snapshots to the configured snapstore according to the specified schedule
- **garbage collection**: garbage-collect old snapshots on the snapstore according to the specified policy
- **defragmentation**: etcd data should be defragmented periodically to reduce db size
- **http server**: http endpoints should work as expected:

  - `/snapshot/full`: should take an on-demand full snapshot
  - `/snapshot/delta`: should take an on-demand delta snapshot
  - `/snapshot/latest`: should list the latest set of snapshots (full + deltas)

- **data validation**: corrupted etcd data should be marked for deletion and restoration should be triggered
- **restoration**: etcd data should be restored correctly from latest set of snapshots (full + deltas)

### Unit tests

Each package within this repo contains its own set of unit tests to test the functionality of the methods contained within the packages.

### Performance regression tests

These tests help check any regression in performance in terms of memory consumption and CPU utilization.
