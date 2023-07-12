# Garbage Collection (GC) Feature

The etcd-backup-restore project incorporates a Garbage Collection (GC) feature designed to manage storage space effectively by systematically discarding older backups. The [`RunGarbageCollector`](pkg/snapshot/snapshotter/garbagecollector.go) function controls this process, marking older backups as disposable and subsequently removing them based on predefined rules.

## GC Policies

Garbage Collection policies fall into two categories, each of which can be configured with appropriate flags:

1. **Exponential Policy**: This policy operates on the principle of retaining the most recent snapshots and discarding older ones, based on the age and capture time of the snapshots. You can configure this policy with the following flag: `--garbage-collection-policy='Exponential'`. The garbage collection process under this policy unfolds as follows:

   - The most recent full snapshot and its associated delta snapshots are perpetually retained, irrespective of the `delta-snapshot-retention-period` setting. This mechanism is vital for potential data recovery.
   - All delta snapshots that fall within the `delta-snapshot-retention-period` are preserved.
   - Full snapshots are retained for the current hour.
   - For the past 24 hours, the most recent full snapshot from each hour is kept.
   - For the past week (up to 7 days), the most recent full snapshot from each day is kept.
   - For the past month (up to 4 weeks), the most recent full snapshot from each week is kept.
   - Full snapshots older than 5 weeks are discarded.

2. **Limit-Based Policy**: This policy aims to keep the snapshot count under a specific limit, as determined by the configuration. The policy prioritizes retaining recent snapshots and eliminating older ones. You can configure this policy with the following flags: `--max-backups=10` and `--garbage-collection-policy='LimitBased'`. The garbage collection process under this policy unfolds as follows:

   - The most recent full snapshot and its associated delta snapshots are always retained, regardless of the `delta-snapshot-retention-period` setting. This is essential for potential data recovery.
   - Full snapshots are retained up to the limit set in the configuration. Any full snapshots beyond this limit are removed.

## Retention Period for Delta Snapshots

The `delta-snapshot-retention-period` setting determines the retention period for older delta snapshots. It does not include the most recent set of snapshots, which are always retained to ensure data safety. The default value for this configuration is 0.

> **Note**: In both policies, the garbage collection process includes listing the snapshots, identifying those that meet the deletion criteria, and then removing them. The deletion operation encompasses the removal of associated chunks, which form parts of a larger snapshot.