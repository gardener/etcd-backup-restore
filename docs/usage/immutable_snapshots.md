# Immutable Snapshots

## Overview of Immutable Objects

Several cloud providers offer functionality to create immutable objects within their storage services. Once an object is uploaded, it cannot be modified or deleted for a set period, known as the **immutability period**. These are referred to as **immutable objects**.

Currently, etcd-backup-restore enables the use of immutable objects on the following cloud platforms:

- Google Cloud Storage  (currently supported)

## Enabling and Using Immutable Snapshots with etcd-backup-restore

Etcd-backup-restore supports immutable objects, typically at what cloud providers call the "bucket level." During the creation of a bucket, it is configured to render objects immutable for a specific duration from the moment of their upload. This feature can be enabled through:

- **Google Cloud Storage**: [Bucket Lock](https://cloud.google.com/storage/docs/bucket-lock)

It is also possible to enable immutability retroactively by making appropriate API calls to your cloud provider, allowing the immutable snapshots feature to be used with existing buckets. For information on such configurations, please refer to your cloud provider's documentation.

The behavior of objects uploaded before a bucket is set to immutable varies among providers. Etcd-backup-restore manages these objects and will perform garbage collection according to the configured policy and the object's immutability expiry.

## Current Capabilities

Etcd-backup-restore does not require configurations regarding the immutability period of an object since this is inherited from the bucket’s immutability settings. The tool also checks the immutability expiry of an object before initiating its garbage collection.

Therefore, it is advisable to configure your garbage collection policies based on the duration you want your objects to remain immutable.

## Storage Considerations

Making objects immutable for extended periods can increase storage costs since these objects cannot be removed once uploaded. Storing outdated snapshots beyond their utility does not significantly enhance recovery capabilities. Therefore, consider all factors before enabling immutability for buckets, as this feature is irreversible once set by cloud providers.
