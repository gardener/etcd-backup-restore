## Prerequisites

Although the following installation instructions are for Mac OS X, similar alternate commands can be found for any Linux distribution.

### Installing [Golang](https://golang.org/) environment

Install the latest version of Golang (at least `v1.12` is required). For Mac OS, you may use [Homebrew](https://brew.sh/):

```sh
brew install golang
```

For other OSes, please check [Go installation documentation](https://golang.org/doc/install).

Make sure to set your `$GOPATH` environment variable properly (conventionally, it points to `$HOME/go`).

For your convenience, you can add the `bin` directory of the `$GOPATH` to your `$PATH`: `PATH=$PATH:$GOPATH/bin`, but it is not mandatory.

### [Golint](https://github.com/golang/lint)

In order to perform linting on the Go source code, please install [Golint](https://github.com/golang/lint):

```bash
go get -u golang.org/x/lint/golint
```

### [Ginkgo](https://onsi.github.io/ginkgo/) and [Gomega](https://onsi.github.io/gomega/)

In order to perform tests on the Go source code, please install [Ginkgo](https://onsi.github.io/ginkgo/) and [Gomega](http://onsi.github.io/gomega/). Please make yourself familiar with both frameworks and read their introductions after installation:

```bash
go get -u github.com/onsi/ginkgo/ginkgo
go get -u github.com/onsi/gomega
```

### Installing `git`

We use `git` as VCS which you would need to install.

On Mac OS run

```sh
brew install git
```

### Installing `gcloud` SDK (Optional)

In case you have to create a new release or a new hotfix, you have to push the resulting Docker image into a Docker registry. Currently, we use the Google Container Registry (this could change in the future). Please follow the official [installation instructions from Google](https://cloud.google.com/sdk/downloads).

## Build

Currently there are no binary builds available, but it is fairly simple to build it by following the steps mentioned below.

* First, you need to create a target folder structure before cloning and building `etcdbrctl`.

    ```sh
    git clone https://github.com/gardener/etcd-backup-restore.git
    cd etcd-backup-restore
    ```

* To build the binary in local machine environment, use `make` target `build-local`. It will build the binary `etcdbrctl` under `bin` directory.

    ```sh
    make build-local
    ```

* Next you can make it available to use as shell command by moving the executable to `/usr/local/bin`, or by optionally including the `bin` directory in your `$PATH` environment variable.
You can verify the installation by running following command:

    ```console
    $ etcdbrctl -v
    INFO[0000] etcd-backup-restore Version: v0.7.0-dev
    INFO[0000] Git SHA: 38979f0
    INFO[0000] Go Version: go1.12
    INFO[0000] Go OS/Arch: darwin/amd64
    ```
