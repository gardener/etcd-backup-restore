# Backup Sync with Dual Site

## Overview

The backup-sync feature in `etcd-backup-restore` provides the ability to synchronize etcd backups (both full and delta snapshots) from a primary storage location to a secondary storage location. This dual-site backup strategy ensures high availability and disaster recovery capabilities by maintaining redundant copies of backups across different storage providers or geographic locations.

## Use Cases

- **Disaster Recovery**: Protect against catastrophic failures of a single storage provider or region
- **Geographic Redundancy**: Store backups in different geographic regions for compliance or availability requirements
- **Multi-Cloud Strategy**: Replicate backups across different cloud providers (e.g., AWS to Azure, GCS to S3)
- **Backup Migration**: Facilitate migration of backups from one storage location to another

## Architecture

The backup-sync functionality is implemented using the `Copier` component, which runs periodically as a background process when the backup-restore server becomes the leading sidecar. The copier monitors the primary snapstore and synchronizes missing snapshots to the secondary snapstore.

### Key Components

1. **Primary Snapstore**: The main storage location where the snapshotter writes backups
2. **Secondary Snapstore**: The destination storage location for synchronized backups
3. **Copier**: The background process that performs periodic synchronization
4. **Snapshotter**: Manages snapshot creation and garbage collection

### Synchronization Flow

```
┌─────────────────┐
│  etcd-backup-   │
│  restore        │
│  (Leader)       │
└────────┬────────┘
         │
         │ Creates snapshots
         ↓
┌────────────────────┐
│ Primary Snapstore  │
│  (e.g., S3)        │
└────────┬───────────┘
         │
         │ Periodic sync
         │ (Copier)
         ↓
┌────────────────────┐
│ Secondary          │
│ Snapstore          │
│ (e.g., Azure Blob) │
└────────────────────┘
```

## Configuration

### Required Flags

To enable backup-sync with dual site, configure the following flags when starting the backup-restore server:

#### Primary Snapstore Configuration

```bash
--storage-provider=<PROVIDER>           # Primary storage provider (S3, ABS, GCS, etc.)
--store-container=<CONTAINER>           # Primary bucket/container name
--store-prefix=<PREFIX>                 # Optional: prefix path in primary store
```

#### Secondary Snapstore Configuration

```bash
--secondary-storage-provider=<PROVIDER>     # Secondary storage provider
--secondary-store-container=<CONTAINER>     # Secondary bucket/container name
--secondary-store-prefix=<PREFIX>           # Optional: prefix path in secondary store
--secondary-backup-sync-enabled=true        # Enable backup synchronization
--secondary-backup-sync-period=<DURATION>   # Sync interval (default: 1h)
```

You can also configure using environment variables. The secondary storage location uses `SECONDARY_` as the prefix. 


### Configuration Example

Here's an example configuration for syncing backups from AWS S3 to Azure Blob Storage:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: etcd-backup-restore
spec:
  containers:
  - name: backup-restore
    image: eu.gcr.io/gardener-project/gardener/etcdbrctl:latest
    command:
    - etcdbrctl
    - server
    - --storage-provider=S3
    - --store-container=primary-etcd-backups
    - --store-prefix=etcd/main
    - --secondary-storage-provider=ABS
    - --secondary-store-container=secondary-etcd-backups
    - --secondary-store-prefix=etcd/main
    - --secondary-backup-sync-enabled=true
    - --secondary-backup-sync-period=5m
    - --max-parallel-copy-operations=10
    env:
    - name: AWS_APPLICATION_CREDENTIALS_JSON
      value: /home/.aws/credentials.json
    - name: SECONDARY_AZURE_APPLICATION_CREDENTIALS_JSON
      value: /home/.azure/credentials.json
    - name: STORAGE_CONTAINER
      value: primary-etcd-backups
    - name: SECONDARY_STORAGE_CONTAINER
      value: secondary-etcd-backups
    volumeMounts:
    - name: aws-credentials
      mountPath: /home/.aws
      readOnly: true
    - name: azure-credentials
      mountPath: /home/.azure
      readOnly: true
```

### Sync Period Configuration

The `--secondary-backup-sync-period` flag controls how frequently the copier synchronizes backups:

- **Default**: 1 hour
- **Considerations**: 
  - Shorter intervals provide more current secondary backups
  - Longer intervals reduce API calls and costs
  - Balance based on your requirements

## Credential Management

Both the primary and secondary snapstores require appropriate credentials. Secondary snapstores can be configured with `SECONDARY_` as a prefix

### AWS S3
```bash
AWS_APPLICATION_CREDENTIALS_JSON=/path/to/aws-credentials.json
SECONDARY_AWS_APPLICATION_CREDENTIALS_JSON=/path/to/secondary-credentials.json
```

### Azure Blob Storage
```bash
AZURE_APPLICATION_CREDENTIALS_JSON=/path/to/azure-credentials.json
SECONDARY_AZURE_APPLICATION_CREDENTIALS_JSON=/path/to/azure-credentials.json
```

### Google Cloud Storage
```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcs-credentials.json
SECONDARY_GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcs-credentials.json
```

### Multiple Providers

When using different providers for primary and secondary storage, ensure both sets of credentials are mounted and accessible:

```yaml
env:
- name: AWS_APPLICATION_CREDENTIALS_JSON
  value: /home/.aws/credentials.json
- name: SECONDARY_AZURE_APPLICATION_CREDENTIALS_JSON
  value: /home/.azure/secondary-credentials.json
```

## Monitoring and Verification

### Logs

Monitor the copier logs to verify synchronization is working:

```
INFO[0000] Starting periodic backup copier..
INFO[0000] Starting backup copier...
INFO[0001] Getting source backups...
INFO[0002] Getting destination snapshots...
INFO[0003] Copying Full snapshot v2/Backup-1234567890/Full-00000000-00000001-1234567890...
INFO[0004] Backups copied
```

### Verification Steps
The examples assume AWS S3 is the backend storage provider. Backups present in other storage provider's bucket like Azure, GCS, Swift etc can be listed using their respective API calls or via their dashboards

1. **Check Primary Backups**:
```bash
aws s3 ls s3://<primary_bucket_name> --recursive
```

2. **Check Secondary Backups**:
```bash
aws s3 ls s3://<secondary_bucket_name> --recursive
```

3. **Compare Counts**: Verify that the number of snapshots in both locations matches (accounting for sync delay)


## Failure Scenarios and Recovery

### Temporary Network Issues

- **Behavior**: Sync fails for current cycle, logs error
- **Recovery**: Automatic retry on next sync cycle
- **Action**: Monitor logs for persistent failures

### Secondary Storage Unavailable

- **Behavior**: Sync operations fail, primary backups continue normally
- **Impact**: No impact on primary backup operations
- **Recovery**: Sync resumes automatically when secondary storage becomes available
- **Action**: Fix secondary storage issues and verify catch-up synchronization

### Authentication Failures

- **Symptoms**: `"failed to create secondary snapstore"` error
- **Causes**: Invalid credentials, expired tokens, missing permissions
- **Resolution**: 
  1. Verify credential files are mounted correctly
  2. Check credential validity and expiration
  3. Ensure IAM/permissions are configured properly
  4. Restart the pod after fixing credentials

### Sync Lag

If secondary backups fall significantly behind:

1. **Identify Cause**: Check logs for recurring errors
2. **Increase Parallelism**: Adjust `--max-parallel-copy-operations`
3. **Reduce Sync Period**: Decrease `--secondary-backup-sync-period` if needed
4. **Manual Sync**: Use the `copy` command for one-time catch-up:
   ```bash
   etcdbrctl copy \
     --storage-provider=S3 \
     --store-container=primary-bucket \
     --secondary-storage-provider=ABS \
     --secondary-store-container=secondary-bucket
   ```

## Garbage Collection for Secondary Buckets

When using dual-site backup synchronization, garbage collection operates independently on both primary and secondary snapstores.
