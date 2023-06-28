
# Garbage Collection (GC) Feature

The etcd-backup-restore project incorporates a Garbage Collection (GC) feature that effectively manages storage space by systematically discarding older backups. The [`RunGarbageCollector`](pkg/snapshot/snapshotter/garbagecollector.go) function orchestrates this process, marking older backups as disposable and subsequently removing them according to predefined rules.

## GC Policies

Garbage Collection policies fall into two categories:

1. **Exponential Policy**: This policy operates on the principle of retaining the most recent snapshots and discarding older ones, with decisions based on the age of snapshots and the time they were captured. The garbage collection process under this policy unfolds as follows:

   - The most recent full snapshot and its associated delta snapshots are perpetually retained, irrespective of the `delta-snapshot-retention-period` setting. This mechanism is vital for potential data recovery.
   - All delta snapshots that fall within the `delta-snapshot-retention-period` are preserved.
   - For the current hour, all full snapshots are maintained.
   - For the past 24 hours, the most recent full snapshot from each hour is kept.
   - For the past week (up to 7 days), the most recent full snapshot from each day is kept.
   - For the past month (up to 4 weeks), the most recent full snapshot from each week is kept.
   - Any full snapshots beyond 5 weeks of age are discarded.

2. **Limit-Based Policy**: This policy ensures that the total number of snapshots does not exceed a specific limit, as defined in the configuration:

   - The most recent full snapshot and its associated delta snapshots are always retained, regardless of the `delta-snapshot-retention-period` setting. This is essential for potential data recovery.
   - Full snapshots are retained up to the limit set in the configuration. Any full snapshots beyond this limit are removed.

## Retention Period for Delta Snapshots

`delta-snapshot-retention-period`: This setting determines the retention period for older delta snapshots. It does not include the most recent set of snapshots, which are always retained to ensure data safety. The default value for this configuration is 0.

> __Note__: In both policies, the garbage collection process includes listing the snapshots, identifying those that meet deletion criteria, and then removing them. The deletion operation encompasses the removal of associated chunks, which form parts of a larger snapshot.

