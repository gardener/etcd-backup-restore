# Immutable Snapshots

## Overview of Immutable Objects

Several cloud providers offer functionality to create immutable objects within their storage services. Once an object is uploaded, it cannot be modified or deleted for a set period, known as the **immutability period**. These are referred to as **immutable objects**.

Currently, etcd-backup-restore supports the use of immutable objects on the following cloud platforms:

- Google Cloud Storage
- Azure Blob Storage

## Enabling and using Immutable Snapshots with etcd-backup-restore

Etcd-backup-restore supports immutable objects, typically at what cloud providers call the "bucket level." During the creation of a bucket, it is configured to render objects immutable for a specific duration from the moment of their upload. This feature can be enabled through:

- **Google Cloud Storage**: [Bucket Lock](https://cloud.google.com/storage/docs/bucket-lock)
- **Azure Blob Storage**: [Container-level WORM Policies](https://learn.microsoft.com/en-us/azure/storage/blobs/immutable-container-level-worm-policies)

It is also possible to enable immutability retroactively by making appropriate API calls to your cloud provider, allowing the immutable snapshots feature to be used with existing buckets. For information on such configurations, please refer to your cloud provider's documentation.

Setting the immutability period at the bucket level through the CLI can be done through the following commands:

- Google Cloud Storage would be done like so:

    ```sh
    gcloud storage buckets update <your-bucket> --retention-period=<desired-immutability-period>
    ```

- Azure Blob Storage Container would be done like so:

    ```sh
    az storage container immutability-policy create --resource-group <your-resource-group> --account-name <your-account-name> --container-name <your-container-name> --period <desired-immutability-period>
    ```

The behaviour of bucket's objects uploaded before a bucket is set to immutable varies among storage providers. etcd-backup-restore manages these objects and will perform garbage collection according to the configured garbage collection policy and the object's immutability expiry.

> Note: If immutable snapshots are not enabled then the object's immutability expiry will be considered as zero, hence causing no effect on current functionality.

## Current Capabilities

etcd-backup-restore does not require configurations related to the immutability period of bucket's objects as this information is derived from the bucket's existing immutability settings. The etcd-backup-restore process also verifies the immutability expiry time of an object prior to initiating its garbage collection.

Therefore, it is advisable to configure your garbage collection policies based on the duration you want your objects to remain immutable.

## Storage Considerations

Making objects immutable for extended periods can increase storage costs since these objects cannot be removed once uploaded. Storing outdated snapshots beyond their utility does not significantly enhance recovery capabilities. Therefore, consider all factors before enabling immutability for buckets, as this feature is irreversible once set by cloud providers.

## Ignoring Snapshots From Restoration

There might be certain cases where operators would like `etcd-backup-restore` to ignore particular snapshots present in the object store during restoration of etcd's data-dir.
When snapshots were mutable, operators could simply delete these snapshots, and the restoration that follows this would not include them.
Once immutability is turned on, however, it would not be possible to do this.

Various cloud providers provide functionality to add custom annotations/tags to objects to add additional information to objects. These additional annotations/tags are orthogonal to the object's metadata, and therefore do not affect the object itself.  This feature is thus available for objects which are immutable as well.

We leverage this feature to signal to etcd-backup-restore to not consider certain snapshots during restoration.
The annotation/tag that is to be added to a snapshot for this is `x-etcd-snapshot-exclude=true`.

You can add these tags through for the following providers like so:

- **Google Cloud Storage**: as specified in the [docs](https://cloud.google.com/sdk/gcloud/reference/storage/objects/update?hl=en). (GCS calls this Custom Metadata).

    ```sh
    gcloud storage objects update gs://bucket/your-snapshot --custom-metadata=x-etcd-snapshot-exclude=true
    ```

    or:

    Use the Google Cloud Console to add custom metadata to the object in the `Custom metadata` section of the object.

- **Azure Blob Storage**: as specified in the [docs](https://learn.microsoft.com/en-us/cli/azure/storage/blob/tag?view=azure-cli-latest#az-storage-blob-tag-set). (ABS calls this tags).

    ```sh
    az storage blob tag set --container-name your-container --name your-snapshot --tags "x-etcd-snapshot-exclude"="true"
    ```

    or

    Use the Azure Portal to add the tag in the `Blob index tags` section of the blob.

Once these annotations/tags are added, etcd-backup-restore will ignore those snapshots during restoration.
