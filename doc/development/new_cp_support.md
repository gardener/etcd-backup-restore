# Adding support for a new object store provider

 Currently the code design only allows in-tree support for different providers. Our roadmap includes the change to code design to allow out-of-tree support for different providers. For adding support for a new object store provider, follow the steps described below. Replace `provider` with your provider-name.

1. Add the provider identifier constant in `pkg/snapstore/types.go`.
1. Add implementation for the `SnapStore` from `pkg/snapstore/types.go` interface  to `pkg/snapstore/provider_snapstore.go`.
    - :warning: Please use environment variable(s) to pass the object store credentials.
    - Provide the factory method to create provider implementation object by loading required access credentials from environment variable(s).
    - Avoid introducing new command line flags for provider.
1. Import the required SDK and any other libraries for provider using `GO111MODULE=on go get <provider-sdk>`. This will update the dependency in [go.mod](../../go.mod) and [go.sum](../../go.sum).
1. Run `make revendor` to download the dependency library to [vendor](../../vendor) directory.
1. Update the [LICENSE.md](../../LICENSE.md) with license details of newly added dependencies.
1. Update the `GetSnapstore` method in `pkg/snapstore/utils.go` to add a new case in switch block to support creation of the new provider implementation object.
1. Add the fake implementation of provider SDK calls under `pkg/snapstore/provider_snapstore_test.go` for unit testing the provider implementation.
1. Register the provider implementation object for testing at the appropriate place under `pkg/snapstore/snapstore_test.go`. This will run generic test against provider implementation.
1. Update the [documentation](../usage/getting_started.md#cloud-provider-credentials) to provide info about passing provider credentials.
1. Update the [helm chart](../../chart/etcd-backup-restore) with provider details.
    - Update the [values.yaml](../../chart/etcd-backup-restore/values.yaml) with configuration for provider.
1. Refer [this commit](https://github.com/gardener/etcd-backup-restore/pull/108/commits/9bcd4e0e96f85ce1f356f08c06a2ced293aaf20b) to for one of the already added provider support.
1. Finally test your code using `make verify`. And raise a PR for review. :smile:
