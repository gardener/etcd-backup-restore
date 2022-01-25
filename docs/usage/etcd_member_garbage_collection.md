## Superfluous member deletion in the ETCD cluster

With support for multi-node operation, there is an expectation that the cluster size can be dynamic. In such scenarios, a reduction in the cluster size should not result in any superfluous members or entries being left over.

Such superfluous members/entries can have an adverse effect on the quorum size of the cluster and could result in a degradation of service.
As such, care has to be taken to periodically check and remove any superfluous entries.

There is the possibility of superfluous entries in the `members` array of the ETCD CRD as well as in the ETCD cluster for which there is no corresponding pod in the `StatefulSet` anymore.
Keeping the `members` array on the ETCD CRD in check is the responsibility of `etcd-druid` whereas keeping the ETCD cluster in check is the responsibility of the leading `etcd-backup-restore` sidecar.
The purpose of this document is the responsibility of the leading `etcd-backup-restore` sidecar to keep the ETCD cluster in check


#### Example
Below is an example of how we could end up with superfluous members in the ETCD cluster.
This is a common scenario that could occur frequently and demonstrates the need for a garbage collection mechanism that detects and clears such superfluous entries automatically
```
Suppose an ETCD cluster is scaled up from `3` to `5` and the newly added members are failing constantly due to insufficient resources and then if the ETCD cluster is scaled back down to `3`. The failing member pods may not have the chance to clean up their `member` entries from the ETCD cluster leading to superfluous members in the ETCD cluster that may have an adverse effect on the quorum of the cluster.
```

### Superfluous member 
A member in the etcd cluster can be considered superfluous only if either of the following 2 cases is true
1. A member does not have a corresponding pod AND a corresponding lease
2. A member is an unstarted learner and is part of the etcd member list without a name

### Approach
- Superfluous members garbage collection can be enabled by setting the `enable-etcd-member-gc` flag to `true`
- The process is only triggered by the leading `etcd-backup-restore` sidecar and not on followers
- The garbage collection mechanism runs periodically with a default time period between runs set to `60s`, but can be modified with the `k8s-member-gc-duration` flag
- It loops through the etcd cluster member list and verifies if a member is considered superfluous by checking if either of the 2 cases mentioned above holds true
- Members considered superfluous are deleted using the etcd cluster API