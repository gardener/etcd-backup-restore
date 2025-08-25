# SSE-C (Server-Side Encryption with Customer-Provided Keys) Support

ETCD backup-restore supports AWS S3 Server-Side Encryption with Customer-Provided Keys (SSE-C) for securing backup snapshots. This allows you to maintain full control over encryption keys while leveraging S3's server-side encryption capabilities.

## Configuration

### Secret Format

SSE-C configuration is provided via Kubernetes secret with the following JSON structure:

```json
{
  "algorithm": "AES256",
  "disableEncryptionForWriting": false,
  "keys": [
    {
      "id": "primary-key-2024",
      "value": "base64-encoded-32-byte-key"
    },
    {
      "id": "backup-key-2023",
      "value": "another-base64-encoded-32-byte-key"
    }
  ]
}
```

#### Field Descriptions

- **`algorithm`** (string, required): SSE-C algorithm to use. Currently only `"AES256"` is supported.
- **`disableEncryptionForWriting`** (boolean, optional, default: `false`): When `true`, new backups will not be encrypted, but existing encrypted backups can still be read using the provided keys.
- **`keys`** (array, required): List of encryption keys. The first key is used for writing new backups.
  - **`id`** (string, required): Unique identifier for the key (used for tracking and logging).
  - **`value`** (string, required): Base64-encoded 32-byte (256-bit) encryption key.

### Multiple Key Support

The system supports multiple SSE-C keys to enable key rotation and backward compatibility:

1. **Writing**: Always uses the **first key** in the array (unless encryption is disabled).
2. **Reading**: Attempts all keys in order until one successfully decrypts the backup.
3. **Key Rotation**: Add new key as the first element, keep old keys for reading existing backups.

### Example Key Rotation Workflow

```json
// Initial configuration
{
  "algorithm": "AES256",
  "keys": [
    {
      "id": "key-2024-01",
      "value": "base64-key-data-here"
    }
  ]
}

// After key rotation - add new key as first element
{
  "algorithm": "AES256", 
  "keys": [
    {
      "id": "key-2024-02",
      "value": "new-base64-key-data-here"
    },
    {
      "id": "key-2024-01", 
      "value": "old-base64-key-data-here"
    }
  ]
}
```

## Flags and Configuration
SSE-C is configured by providing json file `sseCustomerKeyConf` in AWS_SSE_CUSTOMER_KEY_AWS_APPLICATION_CREDENTIALS_JSON path.

### Configuration Behavior

- When SSE-C keys are configured, all backup operations will use SSE-C encryption by default
- Set `disableEncryptionForWriting: true` to stop encrypting new backups while maintaining ability to read existing encrypted backups
- Multiple keys enable seamless key rotation without backup service interruption

## HTTP API

### Get Encryption Status

Returns the current encryption status of all known backup files. This can be out of date if the app didn't yet wrote/read all the files in the store. In such cases you can start asynchronous scan by the `POST /snapshot/scan` endpoint described below.

**Endpoint:** `GET /snapshot/encstatus`

**Requirements:** 
- Only available on leader instances
- Returns 403 Forbidden on non-leader instances

**Response Format:**
```json
{
  "upToDate": true,
  "files": {
    "v2/full/snapshot-1234567890.db": "key-2024-02",
    "v2/delta/snapshot-1234567891.db": "key-2024-01", 
    "v2/full/snapshot-1234567892.db": "unencrypted",
    "v2/delta/snapshot-1234567893.db": "broken"
  },
  "keyStatistics": {
    "key-2024-02": 15,
    "key-2024-01": 8,
    "unencrypted": 2,
    "broken": 1
  }
}
```

**Response Fields:**
- **`upToDate`**: Boolean indicating if the instance already listed all files in the storage, that don't necessarily mean that it knows the status of each file.
- **`files`**: Map of snapshot file paths to their encryption status:
  - Key ID (e.g., `"key-2024-02"`): File encrypted with this key
  - `"unencrypted"`: File is not encrypted with SSE-C
  - `"unknown"`: Encryption status not yet determined
  - `"broken"`: File is encrypted but cannot be decrypted with any available key
- **`keyStatistics`**: Count of files per encryption status (only included when `upToDate` is `true`)

### Scan Encryption Status

Scans all backup files to determine their encryption status.

**Endpoint:** `POST /snapshot/scan`

**Requirements:** 
- Only available on leader instances
- Returns 403 Forbidden on non-leader instances

**Response:** 
```
HTTP 202 Accepted
Snapshot scan started.
```

The scan runs asynchronously and updates the encryption status registry. Use the `/snapshot/encstatus` endpoint to check results.

### Re-encrypt All Backups

Re-encrypts all backups using the current (first) SSE-C key.

**Endpoint:** `POST /snapshot/reencrypt`

**Requirements:**
- Only available on leader instances
- Returns 403 Forbidden on non-leader instances

**Response:**
```
HTTP 202 Accepted
Re-encryption of all snapshots started.
```

The re-encryption process:
1. Skips files already encrypted with the current key
2. Skips unencrypted files if `disableEncryptionForWriting` is `true`
3. Re-encrypts files encrypted with old keys
4. Encrypts unencrypted files if encryption is enabled

## Usage Examples

### Basic SSE-C Setup

1. Create a Kubernetes secret with SSE-C configuration:

```bash
kubectl create secret generic etcd-backup-sse \
  --from-literal=sseCustomerKey='{
    "algorithm": "AES256",
    "keys": [
      {
        "id": "primary-key-2024",
        "value": "'$(openssl rand -base64 32)'"
      }
    ]
  }'
```

2. Configure the backup-restore deployment:

```yaml
env:
- name: AWS_SSE_CUSTOMER_KEY_AWS_APPLICATION_CREDENTIALS_JSON
# this can be part of the secret with AWS S3 bucket credentials or separated,
# but then it needs to be mapped via symlinks to allow updating.
  value: "/path/to/your/directory/with/sseCustomerKeyConf"
```

### Key Rotation Example

1. Check current encryption status:
```bash
curl http://backup-restore-service:8080/snapshot/encstatus
```

2. Update the secret with a new key (as first element):
```bash
kubectl patch secret etcd-backup-sse --type='json' -p='[{
  "op": "replace", 
  "path": "/data/sseCustomerKey",
  "value": "'$(echo '{
    "algorithm": "AES256",
    "keys": [
      {
        "id": "new-key-2024",
        "value": "'$(openssl rand -base64 32)'"
      },
      {
        "id": "primary-key-2024", 
        "value": "previous-base64-key-here"
      }
    ]
  }' | base64 -w 0)'"
}]'
```

3. Re-encrypt all existing backups with the new key:
```bash
curl -X POST http://backup-restore-service:8080/snapshot/reencrypt
```

4. Monitor the re-encryption progress:
```bash
# Check logs or encryption status
curl http://backup-restore-service:8080/snapshot/encstatus
```

### Disabling Encryption for New Backups

To stop encrypting new backups while maintaining access to existing encrypted backups:

```bash
kubectl patch secret etcd-backup-sse --type='json' -p='[{
  "op": "replace",
  "path": "/data/sseCustomerKey", 
  "value": "'$(echo '{
    "algorithm": "AES256",
    "disableEncryptionForWriting": true,
    "keys": [
      {
        "id": "legacy-key-2024",
        "value": "existing-base64-key-here"
      }
    ]
  }' | base64 -w 0)'"
}]'
```

### Scanning for Broken Files

To identify files that cannot be decrypted with available keys:

```bash
# Start scan
curl -X POST http://backup-restore-service:8080/snapshot/scan

# Check results (wait for scan to complete)
curl http://backup-restore-service:8080/snapshot/encstatus | jq '.files | to_entries | map(select(.value == "broken"))'
```

## Security Considerations

1. **Key Management**: Store encryption keys securely using Kubernetes secrets or external key management systems
2. **Key Rotation**: Regularly rotate encryption keys and re-encrypt backups
3. **Access Control**: Limit access to the SSE-C secret and HTTP API endpoints
4. **Backup Keys**: Keep secure backups of encryption keys - losing keys means losing access to encrypted backups

## Troubleshooting

### Common Issues

1. **"broken" files**: Files encrypted with keys no longer available
   - Add missing keys to the configuration (you should have backups)
   - Or accept data loss if keys are permanently lost

2. **"unknown" status**: Encryption status not yet determined
   - Run a scan: `POST /snapshot/scan`
   - Check that the service has leader role

3. **403 Forbidden on API calls**: 
   - Ensure the instance has leader role
   - Check leadership election logs

4. **Re-encryption failures**:
   - Check available disk space
   - Verify all old keys are present in configuration
   - Monitor logs for specific error messages