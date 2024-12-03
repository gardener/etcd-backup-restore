# Enabling Immutable Snapshots in `etcd-backup-restore`

This guide walks you through the process of enabling immutable snapshots in `etcd-backup-restore` by leveraging bucket-level immutability features provided by cloud storage providers like Google Cloud Storage (GCS) and Azure Blob Storage (ABS). Enabling immutability ensures that your backups are tamper-proof and comply with regulatory requirements.

---

## Terminology

- **Bucket / Container**: A storage resource in cloud storage services where objects (such as snapshots) are stored. GCS uses the term **bucket**, while ABS uses **container**.

- **Retention Policy**: A configuration that specifies a minimum period during which objects in a bucket/container are protected from deletion or modification.

- **Retention Period**: The duration defined by the retention policy during which objects remain immutable.

- **Immutability**: A state where objects cannot be modified or deleted until the retention period expires.

- **Locking**: The action of making a retention policy permanent, preventing any reduction or removal of the retention period.

- **ETag**: An identifier representing a specific version of a policy or object, used for concurrency control.

---

## Overview

Currently, `etcd-backup-restore` supports bucket-level immutability for GCS and ABS. Support for AWS S3 is planned for a future release.

- **Retention Policy**: You can add a retention policy to a bucket/container to specify a retention period.
  - When a retention policy is set, objects in the bucket/container can only be deleted or replaced once their age exceeds the retention period.
  - The policy retroactively applies to existing objects as well as new objects added to the bucket/container.

- **Locking a Retention Policy**: You can lock a bucket's/container's retention policy to permanently enforce it.
  - Once locked, you cannot remove the retention policy or reduce its retention period.
  - You cannot delete a bucket/container with a locked retention policy unless every object has met the retention period.
  - You can increase the retention period of a locked policy if needed.

> **Caution**: Locking a retention policy is an irreversible action.

---

## Configure Bucket-Level Immutability

By configuring a retention policy on your storage bucket/container, you ensure that all snapshots are stored in an immutable (Write Once, Read Many) state for a specified duration. This prevents snapshots from being modified or deleted until they reach the end of the retention period.

### Configure a Retention Policy

You can set a time-based retention policy on your bucket/container. The retention policy specifies the minimum duration for which the objects must remain immutable.

#### Google Cloud Storage (GCS)

To configure a retention policy on a GCS bucket:

1. **Set the Retention Policy**

   Use the `gcloud` command-line tool to set the retention period:

   ```bash
   gcloud storage buckets update gs://[BUCKET_NAME] \
       --retention-period [RETENTION_PERIOD]
   ```

   - Replace `[RETENTION_PERIOD]` with the desired retention period (e.g., `4d` for four days, `1y` for one year).
   - Replace `[BUCKET_NAME]` with the name of your bucket.

   **Example:**

   ```bash
   gcloud storage buckets update gs://my-bucket \
       --retention-period 4d
   ```

#### Azure Blob Storage (ABS)

To configure a retention policy on an Azure Blob Storage container:

1. **Create the Retention Policy**

   Use the Azure CLI to create the immutability policy:

   ```bash
   az storage container immutability-policy create \
       --resource-group [RESOURCE_GROUP] \
       --account-name [STORAGE_ACCOUNT] \
       --container-name [CONTAINER_NAME] \
       --period [RETENTION_PERIOD]
   ```

   - Replace `[RESOURCE_GROUP]`, `[STORAGE_ACCOUNT]`, and `[CONTAINER_NAME]` with your specific values.
   - Replace `[RETENTION_PERIOD]` with the desired retention period in days.

   **Example:**

   ```bash
   az storage container immutability-policy create \
       --resource-group myResourceGroup \
       --account-name myStorageAccount \
       --container-name my-container \
       --period 4
   ```

### Modify an Unlocked Retention Policy

You can modify an unlocked retention policy to adjust the retention period or to allow additional writes to the bucket/container.

#### Google Cloud Storage (GCS)

To modify the retention policy:

1. **Set a New Retention Period**

   ```bash
   gcloud storage buckets update gs://[BUCKET_NAME] \
       --retention-period [NEW_RETENTION_PERIOD]
   ```

   **Example:**

   ```bash
   gcloud storage buckets update gs://my-bucket \
       --retention-period 7d
   ```

2. **Remove the Retention Policy**

   ```bash
   gcloud storage buckets update gs://[BUCKET_NAME] \
       --clear-retention-period
   ```

#### Azure Blob Storage (ABS)

To modify an unlocked retention policy:

1. **Retrieve the Policy's ETag**

   ```bash
   etag=$(az storage container immutability-policy show \
           --account-name [STORAGE_ACCOUNT] \
           --container-name [CONTAINER_NAME] \
           --query etag --output tsv)
   ```

2. **Extend the Retention Policy**

   ```bash
   az storage container immutability-policy extend \
       --resource-group [RESOURCE_GROUP] \
       --account-name [STORAGE_ACCOUNT] \
       --container-name [CONTAINER_NAME] \
       --period [NEW_RETENTION_PERIOD] \
       --if-match $etag
   ```

   **Example:**

   ```bash
   az storage container immutability-policy extend \
       --resource-group myResourceGroup \
       --account-name myStorageAccount \
       --container-name my-container \
       --period 7 \
       --if-match $etag
   ```

3. **Delete an Unlocked Policy**

   ```bash
   az storage container immutability-policy delete \
       --resource-group [RESOURCE_GROUP] \
       --account-name [STORAGE_ACCOUNT] \
       --container-name [CONTAINER_NAME] \
       --if-match $etag
   ```

### Lock the Retention Policy

Locking the retention policy makes it irreversible and ensures that the policy cannot be reduced or removed. This provides compliance with regulatory requirements.

#### Google Cloud Storage (GCS)

To lock the retention policy:

1. **Lock the Policy**

   ```bash
   gcloud storage buckets update gs://[BUCKET_NAME] \
       --lock-retention-period
   ```

   **Example:**

   ```bash
   gcloud storage buckets update gs://my-bucket \
       --lock-retention-period
   ```

   **Caution:** Locking is permanent and cannot be undone.

#### Azure Blob Storage (ABS)

To lock the retention policy:

> [!WARNING]
> Once you have thoroughly tested a time-based retention policy, you can proceed to lock the policy. A locked policy ensures compliance with regulations such as SEC 17a-4(f) and other regulatory requirements. While you can extend the retention interval for a locked policy up to **_`five times`_**, it cannot be shortened.
> After a policy is locked, it cannot be deleted. However, you can delete the blob once the retention interval has expired.

1. **Retrieve the Policy's ETag**

   ```bash
   etag=$(az storage container immutability-policy show \
           --account-name [STORAGE_ACCOUNT] \
           --container-name [CONTAINER_NAME] \
           --query etag --output tsv)
   ```

2. **Lock the Policy**

   ```bash
   az storage container immutability-policy lock \
       --resource-group [RESOURCE_GROUP] \
       --account-name [STORAGE_ACCOUNT] \
       --container-name [CONTAINER_NAME] \
       --if-match $etag
   ```

   **Example:**

   ```bash
   az storage container immutability-policy lock \
       --resource-group myResourceGroup \
       --account-name myStorageAccount \
       --container-name my-container \
       --if-match $etag
   ```

   **Caution:** Once locked, the policy cannot be removed or decreased.

---

## Ignoring Snapshots During Restoration

In certain scenarios, you might want `etcd-backup-restore` to ignore specific snapshots present in the object store during the restoration of etcd's data directory. When snapshots were mutable, operators could simply delete these snapshots, and subsequent restorations would not include them. However, once immutability is enabled, it is no longer possible to delete these snapshots.

Many cloud providers allow you to add custom annotations or tags to objects to store additional metadata. These annotations or tags are separate from the object's main data and do not affect the object itself. This feature is available for immutable objects as well.

We leverage this feature to signal to `etcd-backup-restore` to ignore certain snapshots during restoration. The annotation or tag to be added to a snapshot is:

- **Key:** `x-etcd-snapshot-exclude`
- **Value:** `true`

### Adding the Exclusion Tag

#### Google Cloud Storage (GCS)

To add the custom metadata:

1. **Using the gcloud CLI**

   ```bash
   gcloud storage objects update gs://[BUCKET_NAME]/[SNAPSHOT_PATH] \
       --custom-metadata x-etcd-snapshot-exclude=true
   ```

   **Example:**

   ```bash
   gcloud storage objects update gs://my-bucket/snapshots/snapshot.db \
       --custom-metadata x-etcd-snapshot-exclude=true
   ```

2. **Using the Google Cloud Console**

   - Navigate to your bucket in the Google Cloud Console.
   - Locate the snapshot object.
   - Click on the object to view its details.
   - In the **Custom metadata** section, add:
     - **Key:** `x-etcd-snapshot-exclude`
     - **Value:** `true`
   - Save the changes.

#### Azure Blob Storage (ABS)

To add the tag:

1. **Using the Azure CLI**

   ```bash
   az storage blob tag set \
       --account-name [STORAGE_ACCOUNT] \
       --container-name [CONTAINER_NAME] \
       --name [SNAPSHOT_NAME] \
       --tags x-etcd-snapshot-exclude=true
   ```

   **Example:**

   ```bash
   az storage blob tag set \
       --account-name myStorageAccount \
       --container-name my-container \
       --name snapshots/snapshot.db \
       --tags x-etcd-snapshot-exclude=true
   ```

2. **Using the Azure Portal**

   - Navigate to your storage account and container.
   - Locate the snapshot blob.
   - Click on the blob to view its details.
   - In the **Blob index tags** section, add:
     - **Name:** `x-etcd-snapshot-exclude`
     - **Value:** `true`
   - Save the changes.

After adding the annotation or tag, `etcd-backup-restore` will ignore these snapshots during the restoration process.

---

## Setting the Retention Period

When configuring the retention period, consider setting it to align with your delta snapshot retention period, typically **four days**.

- **Why Four Days?** This duration provides a buffer for identifying and resolving issues, especially over weekends.

- **Example Scenario:** If an issue occurs on a Friday, the four-day retention period allows administrators until Monday or Tuesday to debug and recover data without the risk of backups being altered or deleted.

---

## Best Practices

- **Thorough Testing Before Locking:**

  Before locking the retention policy, perform comprehensive testing of your backup and restoration procedures. Ensure that:

  - Snapshots are being created and stored correctly.
  - Restoration from snapshots works as expected.
  - You are familiar with the process of adding exclusion tags if needed.

  Locking the retention policy is irreversible, so it's crucial to confirm that your setup is fully functional.



- **Use Exclusion Tags Wisely:** Apply exclusion tags to snapshots only when absolutely necessary. Overuse of exclusion tags can lead to unintentionally skipping important backups during the restoration process, potentially compromising data integrity.

---

## Conclusion

Enabling immutable snapshots in `etcd-backup-restore` significantly enhances the security and reliability of your backups by preventing unauthorized modifications or deletions. By leveraging bucket/container-level immutability features provided by GCS and ABS, you can meet compliance requirements and ensure data integrity.

It's essential to carefully plan your retention periods and exercise caution when locking retention policies, as these actions have long-term implications. Use the snapshot exclusion feature judiciously to maintain control over your restoration processes.

By following best practices and regularly reviewing your backup and retention strategies, you can ensure that your data remains secure, compliant, and recoverable when needed.

---

**References:**

- **Google Cloud Storage (GCS)**
  - [Bucket Lock Documentation](https://cloud.google.com/storage/docs/bucket-lock)
  - [Use and lock retention policies](https://cloud.google.com/storage/docs/using-bucket-lock)
  - [Custom Metadata](https://cloud.google.com/storage/docs/metadata#custom-metadata)

- **Azure Blob Storage (ABS)**
  - [Immutable Storage for Azure Blob Storage](https://learn.microsoft.com/azure/storage/blobs/immutable-storage-overview)
  - [Configure Immutability Policies](https://learn.microsoft.com/azure/storage/blobs/immutable-policy-configure-container-scope)
  - [Blob Index Tags](https://learn.microsoft.com/azure/storage/blobs/storage-index-tags-overview)

---