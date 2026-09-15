# Dynamic etcd Endpoint Discovery

## Overview

The `ENDPOINTS` file mechanism allows `etcd-backup-restore` to reach etcd when a stable
service endpoint is not available. Instead of a static `--endpoints` flag, `etcd-backup-restore`
reads endpoints from a plain-text file and, optionally, keeps that file up to date by
periodically querying the live etcd member list.

## Enabling the Feature

### 1. Provide the ENDPOINTS file

Set the `ENDPOINTS` environment variable to the path of a file containing one IP address per
line:

```
10.0.0.1
10.0.0.2
10.0.0.3
```

In a Kubernetes pod spec:

```yaml
env:
  - name: ENDPOINTS
    value: /etc/etcd/endpoints
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
```

`POD_IP` is required for single-member bootstrap: if the file is empty (no peers have
registered yet), the pod's own IP is used as the sole endpoint.

### 2. Enable the refresh loop

To keep the file up to date as members join, enable periodic refresh:

```yaml
etcdConnectionConfig:
  endpointsRefreshEnabled: true
  endpointsRefreshInterval: 30s   # must be > 0; default is 30s
```

Or via CLI flags:

```
--enable-endpoints-refresh
--endpoints-refresh-interval=30s
```

When enabled, `etcd-backup-restore` rewrites the `ENDPOINTS` file every interval by querying
the live etcd member list.

## Behaviour Summary

| Condition | Endpoint source |
|---|---|
| `ENDPOINTS` not set | `--endpoints` flags / `etcdConnectionConfig.endpoints` |
| `ENDPOINTS` set, refresh disabled | File is read on demand; content must be kept current externally |
| `ENDPOINTS` set, refresh enabled | File is refreshed automatically every `endpointsRefreshInterval` |

If the `ENDPOINTS` file contains an invalid IP or is unreadable at startup, `etcd-backup-restore`
exits immediately so misconfiguration is caught early.
