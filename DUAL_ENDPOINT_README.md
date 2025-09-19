# Dual S3 Endpoint Backup Support

This document describes how to configure and use the dual S3 endpoint backup feature in etcd-backup-restore.

## Overview

The dual S3 endpoint feature allows you to automatically backup your etcd snapshots to two different S3 endpoints simultaneously. This provides additional redundancy and backup reliability.

## Configuration

### Basic Configuration

To enable dual endpoint backups, add the secondary endpoint parameters to your command:

```bash
etcd-backup-restore snapshot \
  --storage-provider=S3 \
  --store-container=primary-bucket \
  --store-prefix=etcd-backups \
  --secondary-storage-provider=S3 \
  --secondary-store-container=secondary-bucket \
  --secondary-store-prefix=etcd-backups \
  --endpoints=etcd1:2379,etcd2:2379,etcd3:2379
```

### Advanced Configuration

You can also configure different chunk upload settings for each endpoint:

```bash
etcd-backup-restore snapshot \
  --storage-provider=S3 \
  --store-container=primary-bucket \
  --max-parallel-chunk-uploads=5 \
  --min-chunk-size=67108864 \
  --secondary-storage-provider=S3 \
  --secondary-store-container=secondary-bucket \
  --secondary-max-parallel-chunk-uploads=3 \
  --secondary-min-chunk-size=33554432
```

## Credential Configuration

### Primary Endpoint Credentials

Use the existing environment variables for primary endpoint:

```bash
export AWS_APPLICATION_CREDENTIALS=/path/to/primary-credentials.json
# OR
export AWS_ACCESS_KEY_ID=primary-access-key
export AWS_SECRET_ACCESS_KEY=primary-secret-key
export AWS_REGION=us-east-1
```

### Secondary Endpoint Credentials

Use the `SECONDARY_` prefix for secondary endpoint credentials:

```bash
export SECONDARY_AWS_APPLICATION_CREDENTIALS=/path/to/secondary-credentials.json
# OR
export SECONDARY_AWS_ACCESS_KEY_ID=secondary-access-key
export SECONDARY_AWS_SECRET_ACCESS_KEY=secondary-secret-key
export SECONDARY_AWS_REGION=us-west-2
```

### Credential Fallback

If secondary credentials are not provided, the system will fall back to using primary credentials for both endpoints.

## Backup Behavior

### Upload Process

1. **Parallel Upload**: Snapshots are uploaded to both endpoints simultaneously
2. **Success Logging**: Successful uploads to each endpoint are logged
3. **Failure Handling**: If one endpoint fails, the backup continues with the other
4. **Overall Success**: The backup is considered successful if at least one endpoint succeeds

### Sample Logs

```
INFO Successfully saved snapshot Full-00000000-00000010-1640995200 to primary endpoint
INFO Successfully saved snapshot Full-00000000-00000010-1640995200 to secondary endpoint
INFO Snapshot Full-00000000-00000010-1640995200 successfully saved to both endpoints
```

```
INFO Successfully saved snapshot Full-00000000-00000010-1640995200 to primary endpoint
ERRO Failed to save snapshot Full-00000000-00000010-1640995200 to secondary endpoint: connection timeout
WARN Snapshot Full-00000000-00000010-1640995200 saved to only one endpoint - Primary: true, Secondary: false
```

## Restore Behavior

### Endpoint Selection

1. **Cross-endpoint Search**: The restore process searches for snapshots across both endpoints
2. **Primary Preference**: When the same snapshot exists on both endpoints, primary is preferred
3. **Latest Available**: The latest available snapshot across both endpoints is used
4. **Source Logging**: The restore process logs which endpoint provided the snapshot

### Sample Restore Logs

```
INFO Listed 15 snapshots from primary endpoint
INFO Listed 12 snapshots from secondary endpoint
INFO Merged snapshot list: 18 snapshots total (Primary: 15, Secondary: 12)
INFO Successfully fetched snapshot Full-00000000-00000010-1640995200 from primary endpoint
INFO Successfully restored the etcd data directory.
```

## Container Deployment

### Single Container

The dual endpoint feature uses the same container process:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: etcd-backup-restore
spec:
  template:
    spec:
      containers:
      - name: etcd-backup-restore
        image: etcd-backup-restore:latest
        env:
        # Primary credentials
        - name: AWS_APPLICATION_CREDENTIALS
          value: /credentials/primary-creds.json
        # Secondary credentials  
        - name: SECONDARY_AWS_APPLICATION_CREDENTIALS
          value: /credentials/secondary-creds.json
        command:
        - etcd-backup-restore
        - snapshot
        - --storage-provider=S3
        - --store-container=primary-bucket
        - --secondary-storage-provider=S3
        - --secondary-store-container=secondary-bucket
        volumeMounts:
        - name: primary-creds
          mountPath: /credentials/primary-creds.json
          subPath: credentials.json
        - name: secondary-creds
          mountPath: /credentials/secondary-creds.json
          subPath: credentials.json
      volumes:
      - name: primary-creds
        secret:
          secretName: primary-s3-credentials
      - name: secondary-creds
        secret:
          secretName: secondary-s3-credentials
```

## Error Handling

### Upload Failures

- **Single Endpoint Failure**: Backup continues, warning logged
- **Both Endpoints Failure**: Backup fails with detailed error message
- **Partial Success**: Backup succeeds with warning about failed endpoint

### Restore Failures

- **Primary Endpoint Failure**: Automatically tries secondary endpoint
- **Both Endpoints Failure**: Restore fails with detailed error message
- **No Snapshots Found**: Clear error message indicating no backups available

## Monitoring and Alerting

### Key Metrics to Monitor

1. **Backup Success Rate**: Track successful uploads to both endpoints
2. **Endpoint Availability**: Monitor connectivity to both S3 endpoints  
3. **Storage Usage**: Track storage consumption on both endpoints
4. **Restore Success**: Monitor successful restore operations

### Recommended Alerts

- Alert when backups fail on both endpoints
- Alert when one endpoint consistently fails
- Alert when storage usage differs significantly between endpoints

## Best Practices

1. **Different Regions**: Use different AWS regions for primary and secondary endpoints
2. **Different Providers**: Consider using different cloud providers for maximum redundancy
3. **Credential Rotation**: Regularly rotate credentials for both endpoints
4. **Testing**: Regularly test restore operations from both endpoints
5. **Monitoring**: Set up comprehensive monitoring for both endpoints

## Troubleshooting

### Common Issues

1. **Credential Errors**: Verify both primary and secondary credentials are correctly configured
2. **Network Connectivity**: Check connectivity to both S3 endpoints
3. **Bucket Permissions**: Ensure proper read/write permissions on both buckets
4. **Region Mismatch**: Verify bucket regions match credential configurations

### Debug Commands

```bash
# Test primary endpoint
AWS_APPLICATION_CREDENTIALS=/path/to/primary-creds.json aws s3 ls s3://primary-bucket

# Test secondary endpoint  
AWS_APPLICATION_CREDENTIALS=/path/to/secondary-creds.json aws s3 ls s3://secondary-bucket

# List snapshots with verbose logging
etcd-backup-restore snapshot list --log-level=debug
```