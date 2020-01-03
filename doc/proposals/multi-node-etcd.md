# Multi-node etcd cluster backup and restore

## Goal

- Backup etcd cluster deployed with more than one number of nodes to different cloud providers
- Restore the partially down(only some of total etcd nodes down) or fully down (all of the etcd nodes are down) etcd cluster
- For etcd storing only events of Kubernetes cluster, same solution should be able to restart cluster from scratch in case its fully down without having any backup.

## Non-goal

- Recover etcd cluster even if it is deployed across availability zone or across region

## Problem points with extensibility of existing solution

- Bootstrap: etcd needs to know the all members before hand
- Backup-restore sidecar and etcd should be able to talk to each other
- Backup:
    - snapshot should be taken always from the leader in etcd cluster
    - Backup sidecar deployed with each etcd node should coordinate to have single leader pushing snapshot to backup store
- Restore:
    - Minority etcd nodes down let etcd to self heal
    - Majority down restore it from backup store
    - Make use of PV in for optimization purpose


## Design

- Bootstrap
    - We will have fix service DNS and port.
    - Fix number of nodes in etcd cluster.
    - Dynamic changing of number nodes will be out-of-scope.

- Backup:
    - Backup sidecar need to implements explicit leader election independent of etcd
    - Backup thread starts after .
    - :NOTE: George was evaluating this. Ask him.
    - Etcd client redirects all request to leader, so nothing to do much for taking backup only from leader.

- Restore:
    - Need coordination among sidecar

|Events|etcd1-leader|etcd2|etcd3|Backup|Action|
|------|-----|-----|-----|------|------|
|a follower etcd goes down|rev15- up| rev14 up| rev 15- down | rev14| restart etcd and let self heal |
|a leader etcd goes down| rev15- down| rev15up| rev14-up | rev14| restart etcd and let self heal|
|majority etcd down|rev15- down|rev14- down|rev15-up|rev14|restart etcd|
|minority of follower db corrupt| rev15- up| rev14-up| corrupt| rev13|Take current snapshot and restore |
|leader corrupt |corrupt|rev15-up|rev-14up|rev13|Take current snapshot and restore |
|majority db corrupt only follower|rev*|corrupt|corrupt|rev13|take current snapshot and restore entire cluster from that |
|majority db corrupt including leader|corrupt|corrupt|rev*|rev13|***if current revision greater than backup take current snapshot and restore entire cluster from that |

- Health check:
    - Use etcd-druid to have health check on Backup-sidecar
    - Add new internal service over etcd exporting both client port 2379 and server port 2380.
    - Current service which exposes only 2379 client port will be termed as external service.
    - On successful health probe on all backup sidecar the etcd-druid will make this service available
    - On failure of health prove it will update service with addition selector label  `healthy: false` so that the service won't find appropriate etcd pod and fail to serve traffic.