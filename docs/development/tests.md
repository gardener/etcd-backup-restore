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

To run the e2e tests with the emulators, run the following command:

```sh
make ci-e2e-kind PROVIDERS="{providers}"
```

By default, when no provider is specified, the tests are run using AWS emulator i.e Localstack as storage provider. The provider can be specified as comma separated values of the cloud providers mentioned above in small case. For example, to run the tests on AWS and GCP, run the following command:

```sh
make ci-e2e-kind PROVIDERS="aws,gcp"
```

#### Running the e2e tests with real cloud providers

To run the tests with real cloud providers, a few changes need to be made to the configuration file located at `hack/config/<provider>_config.sh` script. The script needs to be updated with the correct credentials for the cloud providers and remove the variables for the emulators. The tests can then be run in the same way as with the emulators.

##### AWS

For AWS, first get the AWS credentials and update the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION` variables of the `hack/config/aws_config.sh` file with the correct values and remove the variables for the Localstack, which are `AWS_ENDPOINT_URL_S3` and `LOCALSTACK_HOST` as also described in the configuration file.

##### GCP

For GCP, first get the GCP credentials and update the `GCP_PROJECT_ID`, `GCP_SERVICE_ACCOUNT_JSON` variables of the `hack/config/gcp_config.sh` file with the correct values and remove the variables for the fake-gcs-server, which are `GOOGLE_EMULATOR_HOST` and `FAKEGCS_LOCAL_URL` as also described in the configuration file.

##### Azure

For Azure, first get the Azure credentials and update the `STORAGE_ACCOUNT`, `STORAGE_KEY` variables of the `hack/config/azure_config.sh` file with the correct values and remove the variables for the Azurite, which are `AZURITE_DOMAIN_LOCAL`, `AZURITE_DOMAIN` and `AZURE_STORAGE_CONNECTION_STRING` as also described in the configuration file.

The e2e tests can also be run on a real cluster by setting the `KUBECONFIG` environment variable to the path of the kubeconfig file of the cluster. The tests can be run in the same way as with the emulators.
