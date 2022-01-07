### Leading ETCD main container’s sidecar is the backup leader 

- The `backup-restore sidecar` poll its corresponding etcd main container to see if it is the leading member in the etcd cluster. This information is used by the backup-restore sidecars to decide that sidecar of the leading etcd main container is the `backup leader`.

- Only `backup leader` sidecar among the members have the responsibility to take/upload the snapshots(full as well as incremental) for a given Etcd cluster as well as to [trigger the defragmentation](https://github.com/gardener/etcd-druid/tree/master/docs/proposals/multi-node#defragmentation) for each Etcd cluster member. 


### Work flow

Backup-restore can be in following 3 states:
1. Follower:
   - When the corresponding etcd main container is a `Follower` etcd.
   - The default state for every backup-restore member is the `Follower` State.
2. Leader:
   - When the corresponding etcd main container becomes the `leader` then sidecar will also become `leading sidecar`.
3. Unknown:
   - When there is no etcd leader present in the cluster or in the case of a quorum loss.
   - When corresponding etcd main container is down and [endpoint status](https://github.com/etcd-io/etcd/blob/f82b5cb7768dacad9fb310232c1383b4e6718378/client/v3/maintenance.go#L53) api call fails.


### Backup

- Only `backup leader` among the backup-restore members have the responsibility to take/upload the snapshots(full as well as incremental) for a given Etcd cluster. 
- The `backup leader` also has the responsibility to [garbage-collect](https://github.com/gardener/etcd-backup-restore/blob/master/doc/usage/getting_started.md#taking-scheduled-snapshot) the backups from the object storage bucket according to a configured garbage collection policy.

### Member-lease

Each backup-restore member has the responsibility to renew its member lease periodically. This is intended to simulate as heartbeat which indicate that the backup-restore cluster members are in `Healthy` State.

### Defragmentation

Defragmentation for all etcd cluster members is triggered by the `leading backup-restore` sidecar. The defragmentation is performed only when etcd cluster is in full health and it is done in a rolling manner for each member to avoid disruption.At first, `leading backup-restore` sidecar triggers defragmentation on all etcd follower members one by one and at last, on itself.


### Complete work flow leader-election state diagram.
![leader-election](../images/leaderElection_stateDiagram.png)

