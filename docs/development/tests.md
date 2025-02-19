# Tests

`etcd-backup-restore` makes use of three sets of tests - unit tests in each package, integration tests to ensure the working of the overall tool and performance regression tests to check changes in resource consumption between different versions of etcd-backup-restore.

### Integration tests

Integration tests include the basic working of:

- **snapshotting**: successfully upload full and delta snapshots to the configured snapstore according to the specified schedule
- **garbage collection**: garbage-collect old snapshots on the snapstore according to the specified policy
- **defragmentation**: etcd data should be defragmented periodically to reduce db size
- **http server**: http endpoints should work as expected:

  - `/snapshot/full`: should take an on-demand full snapshot
  - `/snapshot/delta`: should take an on-demand delta snapshot
  - `/snapshot/latest`: should list the latest set of snapshots (full + deltas)

- **data validation**: corrupted etcd data should be marked for deletion and restoration should be triggered
- **restoration**: etcd data should be restored correctly from latest set of snapshots (full + deltas)

**Note**: The tests expects that the aws credentials are present in the `$HOME/.aws` directory. Make sure to provide the correct credentials before running the tests.

These tests can be run locally with both `etcd` and `etcdbr` running as processes.  To execute the tests, run the following command:

```sh
make integration-test
```

### Unit tests

Each package within this repo contains its own set of unit tests to test the functionality of the methods contained within the packages.

### Performance regression tests

These tests help check any regression in performance in terms of memory consumption and CPU utilization.

### End-to-end tests

The e2e tests for etcd-backup-restore are cluster based tests located in the `test/e2e` package. These tests are run on a Kubernetes cluster and test the full functionality of etcd-backup-restore. The tests create a provider namespace on the cluster and deploy the [etcd-backup-restore helm chart](../../chart/etcd-backup-restore) which in turn deploys the required secrets, configmap, services and finally the statefulset which deploys the pod that runs etcd and backup-restore as a sidecar.

These tests are setup to be run with both emulators and real cloud providers. The emulators can be used for local development and testing as well as running jobs to test code changes when a PR is raised. The real cloud providers can be used for testing in a real cloud environment to ensure that the changes work as expected in an actual environment.

Currently, the tests can be run using the following cloud providers:

- AWS
- GCP
- Azure

#### Running the e2e tests with the emulators

##### On a Kind Cluster

To run the e2e tests with the emulators, run the following command:

```sh
make ci-e2e-kind PROVIDERS="{providers}"
```

By default, when no provider is specified, the tests are run using AWS emulator i.e Localstack as storage provider. The provider can be specified as comma separated values of the cloud providers mentioned above in small case. For example, to run the tests on AWS and GCP, run the following command:

```sh
make ci-e2e-kind PROVIDERS="aws,gcp"
```

##### On any Kubernetes Cluster

The e2e tests can also be run on any other cluster by running the following command:

> **_NOTE:_** If using emulators for e2e tests, make sure to port-forward the snapstore service to the local machine before running the tests.

- For AWS: `kubectl port-forward service/localstack 4566:4566`
- For GCP: `kubectl port-forward service/fake-gcs 4443:4443 8000:8000`
- For Azure: `kubectl port-forward service/azurite 10000:10000`

```sh
make test-e2e PROVIDERS="{providers}" KUBECONFIG="{path-to-kubeconfig}"
```

#### Running the e2e tests with real cloud providers

To run the tests with real cloud providers, the required credentials need to be set as environment variables before running the tests. See the below sections for the required environment variables for each cloud provider. To test with multiple providers, set the required environment variables for each provider.

##### Set the required environment variables

- AWS:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_DEFAULT_REGION`

- GCP:
  - `GOOGLE_APPLICATION_CREDENTIALS`
  - `GCP_PROJECT_ID`

- Azure:
  - `STORAGE_ACCOUNT`
  - `STORAGE_KEY`

##### Run the tests

To run the tests with a kind cluster, run the following command:

```sh
make ci-e2e-kind PROVIDERS="{providers}"
```

To run the tests on any other cluster, run the following command:

```sh
make test-e2e PROVIDERS="{providers}" KUBECONFIG="{path-to-kubeconfig}"
```

