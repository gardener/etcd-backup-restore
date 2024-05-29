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

The Integration tests can be in multiple ways:

  **Note**: The tests expects that the aws credentials are present in the `$HOME/.aws` directory. Make sure to provide the correct credentials before running the tests.

- **Process**: The tests can be run locally with both `etcd` and `etcdbr` running as processes.  To execute the tests, run the following command:

  ```sh
  make integration-test
  ```

- **Cluster**: The tests can be run on a Kubernetes cluster. The tests create a provider specific namespace on the cluster and deploy the [etcd-backup-restore helm chart](../../chart/etcd-backup-restore) which in turn deploys the required secrets, configmap, services and finally the statefulset which contains the pod that runs etcd and backup-restore as a sidecar. To execute the tests, run the following command:

  ```sh
  make integration-test-cluster
  ```
  **Note**: Prerequisite for this command is to set the following environment variables:
  1) INTEGRATION_TEST_KUBECONFIG: kubeconfig to the cluster on which you wish to run the test
  2) ETCD_VERSION: optional, defaults to `v0.1.1`
  3) ETCDBR_VERSION: optional, defaults to `v0.28.0`

### Unit tests

Each package within this repo contains its own set of unit tests to test the functionality of the methods contained within the packages.

### Performance regression tests

These tests help check any regression in performance in terms of memory consumption and CPU utilization.

### End-to-end tests

The e2e tests for etcd-backup-restore are the integrationcluster tests in the `test/e2e/integrationcluster` package. These tests are run on a Kubernetes cluster and test the full functionality of etcd-backup-restore. The tests create a provider namespace on the cluster and deploy the [etcd-backup-restore helm chart](../../chart/etcd-backup-restore) which in turn deploys the required secrets, configmap, services and finally the statefulset which deploys the pod that runs etcd and backup-restore as a sidecar.

These tests are setup to be run with both emulators and real cloud providers. The emulators can be used for local development and testing as well as prow job to test code changes when a PR is raised. The real cloud providers can be used for testing in a real cloud environment to ensure that the changes work as expected in a real environment. 

Currently the tests are run on the following cloud providers: 
- AWS
- GCP
- Azure

To run the e2e tests with the emulators, run the following command:

```sh
make ci-e2e-kind PROVIDERS="{providers}"
```

By default, when no provider is specified, the tests are run using AWS emulator i.e Localstack as storage provider. The provider can be specified as comma separated values of the cloud providers mentioned above in small case. For example, to run the tests on AWS and GCP, run the following command:

```sh
make ci-e2e-kind PROVIDERS="aws,gcp"
```


To run the tests with real cloud providers, a few changes need to be made to the `hack/ci-e2e-kind.sh` script. The script needs to be updated with the correct credentials for the cloud providers and remove the variables for the emulators.

#### AWS

For AWS, first get the AWS credentials and update the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION` variables of the `hack/ci-e2e-kind.sh` script with the correct values along with removing the variables for the Localstack provider, which are `AWS_ENDPOINT_URL_S3` and `LOCALSTACK_HOST` from the make command of the `hack/ci-e2e-kind.sh` script. Then should also update the creation of `/tmp/aws.json` file with the correct values. It should look like below snippet after removing the `endpoint` and `s3ForcePathStyle` fields: 
  
  ```sh
  export AWS_APPLICATION_CREDENTIALS_JSON="/tmp/aws.json"
echo "{ \"accessKeyID\": \"${AWS_ACCESS_KEY_ID}\", \"secretAccessKey\": \"${AWS_SECRET_ACCESS_KEY}\", \"region\": \"${AWS_DEFAULT_REGION}\" }" > "${AWS_APPLICATION_CREDENTIALS_JSON}"
  ```

With these changes made, the tests can be run in the same way as with the emulators.

#### GCP

For GCP, first get the GCP credential service account json file and replace the `GOOGLE_APPLICATION_CREDENTIALS` with the path to this service account file. And also update the `GCP_PROJECT_ID` variable and set it to your project ID. We also need to remove the required environment variables for the fakegcs provider from the make command of the `hack/ci-e2e-kind.sh` file, for that one should remove the variables `GOOGLE_EMULATOR_ENABLED`, `GCS_EMULATOR_HOST` and `GOOGLE_STORAGE_API_ENDPOINT` and run the tests in the same way as with the emulators.

#### Azure

For Azure, first get the Azure credentials and update the `STORAGE_ACCOUNT` and `STORAGE_KEY` env variables in the `hack/ci-e2e-kind.sh` script with the correct values. Also, remove the variables for the azurite, which are `AZURE_STORAGE_API_ENDPOINT`, `AZURE_EMULATOR_ENABLED`, `AZURITE_HOST` and `AZURE_STORAGE_CONNECTION_STRING` from the make command of the `hack/ci-e2e-kind.sh` script. With these changes made, the tests can be run in the same way as with the emulators.

The e2e tests can also be run on a real cluster by setting the `KUBECONFIG` environment variable to the path of the kubeconfig file of the cluster. The tests can be run in the same way as with the emulators.
