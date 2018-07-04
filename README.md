# Etcd-Backup-Restore

[![CI Build status](https://concourse.ingress.ci-2.infra.gardener.cloud/api/v1/teams/gardener/pipelines/etcd-backup-restore-master/jobs/etcd-backup-restore-master-master-head-update-job/badge)](https://concourse.ci.infra.gardener.cloud/teams/gardener/pipelines/etcd-backup-restore-master/jobs/etcd-backup-restore-master-master-head-update-job)
[![Go Report Card](https://goreportcard.com/badge/github.com/gardener/etcd-backup-restore)](https://goreportcard.com/report/github.com/gardener/etcd-backup-restore)
[![GoDoc](https://godoc.org/github.com/gardener/etcd-backup-restore?status.svg)](https://godoc.org/github.com/gardener/etcd-backup-restore)

Etcd-backup-restore is collection of components to backup and restore the [etcd]. It also, provides the ability to validate the data directory, so that we could know the data directory is in good shape to bootstrap etcd successfully.

## Table of Contents

- [Getting started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Build](#build)
- [Design](#design)
- [Usage](#usage)
  - [Cloud Provider Credentials](#cloud-provider-credentials)
  - [Taking scheduled snapshot](#taking-scheduled-snapshot)
  - [Etcd data directory initialization](#etcd-data-directory-initialization)
  - [Etcdbrctl server](#etcdbrctl-server)
- [Dependency management](#dependency-management)
  - [Updating dependencies](#updating-dependencies)
- [Testing](#testing)

## Getting started

Currently there are no binary build available, but it is pretty straight forward to build it by following the steps mentioned below.

### Prerequisites

Although the following installation instructions are for Mac OS X, similar alternate commands could be found for any Linux distribution

#### Installing [Golang](https://golang.org/) environment

Install the latest version of Golang (at least `v1.9.4` is required). For Mac OS, you could use [Homebrew](https://brew.sh/):

```sh
brew install golang
```

For other OS, please check [Go installation documentation](https://golang.org/doc/install).

Make sure to set your `$GOPATH` environment variable properly (conventionally, it points to `$HOME/go`).

For your convenience, you can add the `bin` directory of the `$GOPATH` to your `$PATH`: `PATH=$PATH:$GOPATH/bin`, but it is not necessarily required.

We use [Dep](https://github.com/golang/dep) for managing golang package dependencies. Please install it
on Mac OS via

```sh
brew install dep
```

On other operating systems, please check the [Dep installation documentation](https://golang.github.io/dep/docs/installation.html) and the [Dep releases page](https://github.com/golang/dep/releases). After downloading the appropriate release in your `$GOPATH/bin` folder, you need to make it executable via `chmod +x <dep-release>` and rename it to dep via `mv dep-<release> dep`.

#### [Golint](https://github.com/golang/lint)

In order to perform linting on the Go source code, please install [Golint](https://github.com/golang/lint):

```bash
go get -u github.com/golang/lint/golint
```

#### [Ginkgo](https://onsi.github.io/ginkgo/) and [Gomega](https://onsi.github.io/gomega/)

In order to perform tests on the Go source code, please install [Ginkgo](https://onsi.github.io/ginkgo/) and [Gomega](http://onsi.github.io/gomega/). Please make yourself familiar with both frameworks and read their introductions after installation:

```bash
go get -u github.com/onsi/ginkgo/ginkgo
go get -u github.com/onsi/gomega
```

#### Installing `git`

We use `git` as VCS which you would need to install.

On Mac OS run

```sh
brew install git
```

#### Installing `gcloud` SDK (Optional)

In case you have to create a new release or a new hotfix, you have to push the resulting Docker image into a Docker registry. Currently, we are using the Google Container Registry (this could change in the future). Please follow the official [installation instructions from Google](https://cloud.google.com/sdk/downloads).

### Installing `Docker` (Optional)

In case you want to build Docker images, you have to install Docker itself. We recommend using [Docker for Mac OS X](https://docs.docker.com/docker-for-mac/) which can be downloaded from [here](https://download.docker.com/mac/stable/Docker.dmg).

### Build

First, you need to create a target folder structure before cloning and building `etcdbrctl`.

```sh

mkdir -p ~/go/src/github.com/gardener
cd ~/go/src/github.com/gardener
git clone git@github.com:gardener/etcd-backup-restore.git
cd etcd-backup-restore
```

To build the binary in local machine environment, use `make` target `build-local`. 

```sh
make build-local
```

This will build the binary `etcdbrctl` under `bin` directory.

Next you can make it available to use as shell command by moving the executable to `/usr/local/bin`.

## Design

Please find the design doc [here](doc/design.md).

## Usage

You can follow the `help` flag on `etcdbrctl` command and its sub-commands to know the usage details. Some of the common use cases are mentioned below. Although examples below uses AWS S3 as storage provider, we have added support for AWS, GCS, Azure and Openstack swift object store. It also supports local disk as storage provider.

### Cloud Provider Credentials

The procedure to provide credentials to access the cloud provider object store varies for different providers. 

For `AWS S3`, the `credentials` file has to be provided in the `~/.aws` directory.

For `GCP Containers`, the service account json file should be provided in the `~/.gcp` as a `service-account-file.json` file.

For `Azure Blob storage`, `STORAGE_ACCOUNT` and `STORAGE_KEY` should be made available as environment variables.

For `Openstack Swift`, `OS_USERNAME`, `OS_PASSWORD`, `OS_AUTH_URL`, `OS_TENANT_ID` and `OS_DOMAIN_ID` should be made available as environment variables.

### Taking scheduled snapshot

`etcd` should already be running. One can apply standard cron format scheduling for regular backup of etcd.

```sh
$ ./bin/etcdbrctl snapshot --storage-provider="S3" --etcd-endpoints http://localhost:2379 --max-backups=7 --schedule "* */1 * * *" --store-container="etcd-backup"
INFO[0000] Validating schedule...
INFO[0000] Will take next snapshot at time: 2018-03-27 17:36:00 +0530 IST
INFO[0010] Taking scheduled snapshot for time: 2018-03-27 17:36:00.004816695 +0530 IST
INFO[0010] Successfully opened snapshot reader on etcd
INFO[0010] Successfully saved full snapshot at: Full-00000000-00040010-1522152360
INFO[0010] Executing garbage collection...
INFO[0010] Will take next snapshot at time: 2018-03-27 17:37:00 +0530 IST
```

The command mentioned above takes hourly snapshots and pushs it to S3 bucket named "etcd-backup". It is configured to keep only last 7 backups in bucket. 

### Etcd data directory initialization

Sub-command `initialize` does the task of data directory validation. If the data directory is found to be corrupt, the controller will restore it from the latest snapshot in the cloud store.

```sh
$ ./bin/etcdbrctl initialize --storage-provider="S3" --store-container="etcd-backup" --data-dir="default.etcd"
INFO[0000] Checking for data directory structure validity...
INFO[0000] Checking for data directory files corruption...
INFO[0000] Verifying snap directory...
Verifying Snapfile default.etcd/member/snap/0000000000000001-0000000000000001.snap.
INFO[0000] Verifying WAL directory...
INFO[0000] Verifying DB file...
INFO[0000] Data directory corrupt. Invalid db files: invalid database
INFO[0000] Removing data directory(default.etcd) for snapshot restoration.
INFO[0000] Finding latest snapshot...
INFO[0000] Restoring from latest snapshot: Full-00000000-00040010-1522152360...
2018-03-27 17:38:06.617280 I | etcdserver/membership: added member 8e9e05c52164694d [http://localhost:2380] to cluster cdf818194e3a8c32
INFO[0000] Successfully restored the etcd data directory.
```

### Etcdbrctl server

With sub-command `server` you can start a http server which exposes an endpoint to initialize etcd over REST interface. The server also keeps on backup schedule thread running to have periodic backups. This is mainly made available to manage an etcd instance running in a Kubernetes cluster. You can deploy the example [manifest](./example/etcd-statefulset.yaml) on a Kubernetes cluster to have an fault resilient etcd instance.

## Dependency management

We use [Dep](https://github.com/golang/dep) to manage golang dependencies.. In order to add a new package dependency to the project, you can perform `dep ensure -add <PACKAGE>` or edit the `Gopkg.toml` file and append the package along with the version you want to use as a new `[[constraint]]`.

### Updating dependencies

The `Makefile` contains a rule called `revendor` which performs a `dep ensure -update` and a `dep prune` command. This updates all the dependencies to its latest versions (respecting the constraints specified in the `Gopkg.toml` file). The command also installs the packages which do not already exist in the `vendor` folder but are specified in the `Gopkg.toml` (in case you have added new ones).

```sh
make revendor
```

The dependencies are installed into the `vendor` folder which **should be added** to the VCS.

:warning: Make sure you test the code after you have updated the dependencies!

## Testing

We have created `make` target `verify` which will internally run different rule like `fmt` for formatting, `lint` for linting check and most importantly `test` which will check the code against predefined tests. Although, currently there are not enough test cases written to cover entire code, hence one should check for failure cases manually before raising pull request. We will eventually add the test cases for complete code coverage.

```sh
make verify
```

By default, we try to run test in parallel without computing code coverage. To get the code coverage, you will have to set environment variable `COVER` to `true`. This will log the code coverage percentage at the end of test logs. Also, all cover profile files will accumulated under `test/output/coverprofile.out` directory. You can visualize exact code coverage using `make show-coverage`.

Similarly, we use environment variable `INTEGRATION` to determine whether to execute integration test or not. By default, integration tests are executed. So, to disable integration test locally, you will have to set `INTEGRATION=false`.

[etcd]: https://github.com/coreos/etcd
