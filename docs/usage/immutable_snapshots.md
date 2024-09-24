# Immutable Snapshots

## Overview of Immutable Objects

Several cloud providers offer functionality to create immutable objects within their storage services. Once an object is uploaded, it cannot be modified or deleted for a set period, known as the **immutability period**. These are referred to as **immutable objects**.

Currently, etcd-backup-restore supports the use of immutable objects on the following cloud platforms:

- Google Cloud Storage  (currently supported)

## Enabling and using Immutable Snapshots with etcd-backup-restore

Etcd-backup-restore supports immutable objects, typically at what cloud providers call the "bucket level." During the creation of a bucket, it is configured to render objects immutable for a specific duration from the moment of their upload. This feature can be enabled through:

- **Google Cloud Storage**: [Bucket Lock](https://cloud.google.com/storage/docs/bucket-lock)

It is also possible to enable immutability retroactively by making appropriate API calls to your cloud provider, allowing the immutable snapshots feature to be used with existing buckets. For information on such configurations, please refer to your cloud provider's documentation.

The behaviour of bucket's objects uploaded before a bucket is set to immutable varies among storage providers. Etcd-backup-restore manages these objects and will perform garbage collection according to the configured garbage collection policy and the object's immutability expiry.

> Note: If immutable snapshots are not enabled then the object's immutability expiry will be considered as zero, hence causing no effect on current functionality.

## Current Capabilities

Etcd-backup-restore does not require configurations related to the immutability period of bucket's objects as this information is derived from the bucket's existing immutability settings. The etcd-backup-restore process also verifies the immutability expiry time of an object prior to initiating its garbage collection.

Therefore, it is advisable to configure your garbage collection policies based on the duration you want your objects to remain immutable.

## Storage Considerations

Making objects immutable for extended periods can increase storage costs since these objects cannot be removed once uploaded. Storing outdated snapshots beyond their utility does not significantly enhance recovery capabilities. Therefore, consider all factors before enabling immutability for buckets, as this feature is irreversible once set by cloud providers.
