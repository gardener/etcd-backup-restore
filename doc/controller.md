# A case for an etcd controller

## Abstract

This document tries to show that introducing a Kubernetes controller would increase the options for solving the various problems encountered in the life-cycle management of `etcd` instances.

## Goal

To justify the introduction of a Kubernetes controller into the life-cycle management of `etcd` instances in terms of the increased number of options to solve the known problems in the domain.

## Non-goal

It is not a goal of this document to recommend rewriting of all the current functionality in etcd-backup-restore as a Kubernetes [operator](https://coreos.com/operators/).
The options highlighted below are just that. Options. 

Even if there is a consensus on adopting the controller way to implement some subset of the functionality below, it is not the intention of this document to advocate for implementing all those features right away.

## Content

* [A case for an etcd controller](#a-case-for-an-etcd-controller)
  * [Abstract](#abstract)
  * [Goal](#goal)
  * [Non\-goal](#non-goal)
  * [Content](#content)
  * [Single\-node and Multi\-node](#single-node-and-multi-node)
  * [Database Restoration](#database-restoration)
  * [Database Incremental Backup Compaction](#database-incremental-backup-compaction)
  * [Database Verification](#database-verification)
  * [Autoscaling](#autoscaling)
  * [Non\-disruptive Maintenance](#non-disruptive-maintenance)
  * [Co\-ordination outside of Gardener Reconciliation](#co-ordination-outside-of-gardener-reconciliation)
  * [Backup Health Verification](#backup-health-verification)
  * [Major Version Upgrades](#major-version-upgrades)
  * [Summary](#summary)

## Single-node and Multi-node

We currently support only single-node `etcd` instances.
There might be valid reasons to also support multi-node `etcd` clusters including the non-disruptive maintenance and support for Gardener Ring.

A multi-node `etcd` cluster requires co-ordination between the `etcd` nodes not just for consensus management but also for life-cycle management tasks.
This means that co-ordination is required across nodes for some (or all) tasks such as backups (full and delta), verification, restoration and scaling.
Some of that co-ordination could be done with the current sidecar approach.
But many of them, especially, restoration and scaling are done better with a controller.

For backups, a better option might be to separate the backup sidecar as a separate pod for the whole etcd cluster rather than a sidecar container for each etcd instance in the etcd cluster.
This could be optionally used only in the multi-node scenario and we can perhaps continue to use the sidecar container approach for the single-node scenario.
This could be the first step and we can think of introducing the controller for co-ordination for other life-cycle operations later.

## Database Restoration

Database restoration is also currently done on startup (or a restart) (if database verification fails) within the same backup-restore sidecar's main process.

Introducing a controller enables the option to perform database restoration as a separate job.
The main advantage of this approach is to decouple the memory requirement of a database restoration from the regular backup (full and delta) tasks.
This could be especially of interest because the delta snapshot restoration requires an embedded `etcd` instance which might mean that the memory requirement for database restoration is almost certain to be proportionate to the database size. However, the memory requirement for backup (full and delta) need not be proportionate to the database size at all. In fact, it is very realistic to expect that the memory requirement for backup be more or less independent of the database size.

The current backup-restore component already supports a subcommand for restoration. So, the existing functionality for restoration can be fully reused even at the level of the binary docker image.
The additional co-ordination steps to scale down the statefulset, trigger the restoration job, wait for the completion of restoration and scaling back the statefulset would have to be encoded in a controller.

This could be a second step because we can introduce only a lean controller for restoration and it might even be possible to do this without introducing a CRD.

## Database Incremental Backup Compaction

Incremental/continuous backup is used for finer granularity backup (in the order of minutes) with full snapshots being taken at a much larger intervals (in the order of hours). This makes the backup efficient both in terms of disk, network bandwidth and backup storage space utilization as well as compute resource utilization during backup.

However, if the proportion of changes in the incremental backup is large then this impacts the restoration times because incremental backups can only be restored in sequence as is mentioned in [#61](https://github.com/gardener/etcd-backup-restore/issues/61).

A controller can be used to periodically perform compaction of the incremental backups in the backup storage. This can optimize both the restoration times as well as the backup storage space utilization while not affecting the regular backup performance because such compaction would be done asynchronously.

This feature could be implemented as an enhancement of the [database restoration](#database-restoration) functionality.

## Database Verification

Database verification is currently done on startup (or a restart) within the same backup-restore sidecar's main process.
There is a co-ordination shell script that makes sure that the etcd process is not started until verification is completed successfully.

Introducing a controller enables the option to perform the database verification only when needed as a separate job.
This has two advantages.
1. Decouple database verification from `etcd` restart. This has the potential to avoid unnecessary delays during every single `etcd` restart.
1. Decouple the memory requirement of a database verification from the regular backup (full and delta) tasks. This has the potential to reduce the memory requirement of the backup sidecar and isolate the memory spike of a database verification.

Of course, it is possible to decouple database verification from `etcd` restart without introducing the controller but would need the co-ordination shell script to be more complicated as can be seen in [#93](https://github.com/gardener/etcd-backup-restore/pull/93). Such complicated shell scripts are generally better avoided. Especially, when they are not even part of any docker image.

Another alternative is to create a custom image for etcd container to include the co-ordination logic. This is also not very desirable.

As mentioned in the case of [database restoration](#database-restoration), database verification is a part of a subcommand of the existing backup-restore component. So, the existing verification functionality can be fully reused even at the level of the binary docker image.

## Autoscaling

The [`VerticalPodAutoscaler`](https://github.com/kubernetes/autoscaler/tree/master/vertical-pod-autoscaler) supports multiple update policies including `recreate`, `initial` and `off`.
The `recreate` policy is clearly not suitable for a single-node `etcd` instances because of the implications on frequent and unpredictable down-time.
The `initial` policy makes more sense when coupled with unlimited resource limits (but very clear autoscaled resource requests).

With a controller, even the `off` option becomes feasible because the time for applying `VerticalPodAutoscaler`'s could be decided in a very custom way while still relying on the recommendations from the `VerticalPodAutoscaler`.

## Non-disruptive Maintenance

We currently support only single-node `etcd` instances.
So, any change to the `etcd` `StatefulSet` or its pods have a disruptive impact with down-time implication.

One way to make such changes less disruptive could be to temporarily scale the cluster into a multi-node (3) `etcd` cluster, perform the disruptive change on a rolling manner on each of the individual nodes of the multi-node `etcd` clusters and then scale down the `etcd` back to single-instance.
This kind of multi-step process is better implemented as a controller.

## Co-ordination outside of Gardener Reconciliation

Currently, the `etcd` `StatefulSet` is provisioned by Gardener and this is the only point of co-ordination for the `etcd` life-cycle management.
This couples the `etcd` life-cycle management with Gardener Reconciliation.

Because of the disruptive nature of scaling of single-node `etcd` instances, it might make sense to restrict some of the low priority life-cycle operations (for example, scaling down) to the maintenance window of the `Shoot` cluster which is backed by the given `etcd` instance.
It would be possible to implement this with the current sidecar approach but might be cleaner to do it as controller (also, possibly a `CustomResourceDefinition` to capture co-ordination information such as maintenance window).

## Backup Health Verification

Currently, we rely on the database backups in the storage provider to remain healthy. There are no additional checks to verify if the backups are still healthy after upload.
A controller can be used to perform such backup health verification asynchronously.

This is a minor point which may not be important to implement.

## Major Version Upgrades

So far, `etcd` versions we have been using have provided backward compatibility for the databases. But it is possible that in some future version there is a break in compatibility and some data migration is required. Without a controller, this would have to be done in an ad hoc manner. A controller can provide a good place to encode such logic.

This feature could be implemented on demand when a new etcd version requires such data migration.

## Summary

As seen above, many of the problems encountered in the life-cycle management of `etcd` clusters can be addressed with just the current sidecar approach without introducing a controller.
However, introducing a controller provides many more alternative approaches to implement solutions to these problems while not removing the possibility of using the existing sidecar approaches.
It can be argued that some of the problems such as restoration, backup compaction, non-disruptive maintenance, multi-node and co-ordination outside of Gardener reconciliation are done better with a controller.

To be clear, introducing a controller need not preclude the continued use of the existing sidecar approach. For example, backups (full and delta) are probably done better via a sidecar.
