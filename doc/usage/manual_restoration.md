# Manual restoration of etcd data

Please make sure that you have read through the [documentation on etcd data restoration](../proposals/restoration.md) before reading this document and attempting to perform a manual restoration of etcd data.

As mentioned in previously, automatic restoration will be triggered if the etcd data gets corrupted, as the etcd process will crash. But if for some reason, restoration is not automatically triggered when it should have, you may choose to manually restore the etcd data.

## Steps to perform restoration

You may choose to follow different methods of restoration, based on your etcd + backup sidecar setup:

1. Deploying the [provided helm chart](../../chart/etcd-backup-restore), in which etcdbrctl is started in `server` mode
    1. Exec into the `etcd` container of the `main-etcd-0` pod and delete the `member` directory under the data directory in order to invalidate it
        - `rm -rf /var/etcd/data/new.etcd/member`
        - You may choose to rename the `member` directory instead of deleting it, if you wish to retain the old data for debugging
    1. This will crash the etcd container and when it restarts, the backup sidecar will perform a validation of the data directory, and seeing that the data is corrupt, it will restore the data from the latest backup
    - :warning: Keep in mind that the latest backup in the object storage bucket might not be up-to-date with the latest etcd data, and you could see a maximum data loss corresponding to the delta snapshot interval. For instance, setting `delta-snapshot-interval=5m` could result in a maximum data loss worth 5 minutes.

1. Deploying etcd and etcdbrctl separately, where etcdbrctl is started in `server` mode
    1. If using [this bootstrap script](../../chart/etcd-backup-restore/templates/etcd-bootstrap-configmap.yaml) for starting etcd, then deleting the `member` directory under the etcd data directory should kill the etcd process, and subsequently the script finishes execution and exits. You will have to re-run the script and allow it to trigger data validation anf restoration by etcdbrctl.
    1. If not using the bootstrap script, then:
        1. Delete the `member` directory and wait for etcd to crash
        1. `curl http://localhost:8080/initialization/status`, assuming etcdbrctl is running on port 8080
        1. `curl http://localhost:8080/initialization/start`
        1. Wait for the restoration to finish, by observing the logs from etcdbrctl
        1. Again, `curl http://localhost:8080/initialization/status` to complete the initialization process, and etcdbrctl will resume regular snapshotting after this

1. Deploying etcd and etcdbrctl separately, where etcdbrctl is started in `snapshot` mode
    1. Delete the `member` directory and wait for etcd to crash
    1. Kill the etcdbrctl process
    1. Run etcdbrctl in `restore` mode to perform a restoration of the data
        - It is highly recommended to run etcdbrctl in `initialize` mode rather than `restore` mode, as this performs the necessary validation checks on the data directory taking the decision to trigger a restoration.
    1. Start etcd again
    1. Restart etcdbrctl in `snapshot` mode
    - :warning: If running etcdbrctl in `snapshot` mode, it is necessary to stop this snapshotter process before triggering a restoration, to avoid data inconsistency.

:warning: If in doubt about the validity of the etcd data, operators must first visually confirm any inconsistency in revision numbers between the latest snapshot-set in the object store and the running etcd instance using the `etcdctl` tool before deciding to perform a manual restoration.

:warning: In order to successfully perform a restoration, the data directory must NOT contain the `member` directory, else the restoration will fail.

:warning: **Do not tamper with the object store in any way.** Data once lost from the object store, cannot be recovered. The object store is considered as the source of truth for the restorer.
