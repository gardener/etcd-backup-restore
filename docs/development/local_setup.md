# Local Setup

## Prerequisites

Although the following installation instructions are for macOS, similar alternate commands can be found for any Linux distribution, and using `winget` or `chocolatey` for Windows.

### Installing [Golang](https://golang.org/) environment

Install the latest version of Golang (at least `v1.20` is required). For macOS, you may use [Homebrew](https://brew.sh/):

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

On macOS run

```sh
brew install git
```

### Installing `gcloud` SDK (Optional)

In case you have to create a new release or a new hotfix, you have to push the resulting Docker image into a Docker registry. Currently, we use the Google Container Registry (this could change in the future). Please follow the official [installation instructions from Google](https://cloud.google.com/sdk/downloads).

## Build

Currently, there are no binary builds available, but it is fairly simple to build it by following the steps mentioned below.

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
    ➜ etcdbrctl -v
    INFO[0000] etcd-backup-restore Version: v0.29.0-dev     
    INFO[0000] Git SHA: 0247d8d4                            
    INFO[0000] Go Version: go1.22.1                         
    INFO[0000] Go OS/Arch: darwin/arm64
    ```

## Running `etcdbrctl` as a process

> [!WARNING]  
> Ensure that you do not add your `credentialDirectory` or `credential_file.json` to your version control. Easiest way to ensure this, is to avoid using `git add .`. You could add these files/directory as `.gitignore` targets, or have the credential file/directory exist in a directory which is not tracked by `git`.

### Environment variables

Taking the example of S3 here, one has to:

* Set the `AWS_APPLICATION_CREDENTIALS` environment variable if the credentials are to be stored in a directory, where each file in the directory holds a different credential
* Set the `AWS_APPLICATION_CREDENTIALS_JSON` if the credentials are to be stored in `.json` file.

Export the following environment variables according to your host OS:

* `ETCD_CONF=example/01-etcd-config.yaml`
* `POD_NAME=POD_0`
* `POD_NAMESPACE=POD_NAMESPACE_0`
* One of the following based on the authentication type:
  * `AWS_APPLICATION_CREDENTIALS=/path/to/your/credential/file/directory/s3credentials`
  * `AWS_APPLICATION_CREDENTIALS_JSON=/path/to/your/credential/file/credential_file.json`

> For example: `export ETCD_CONF=example/01-etcd-config.yaml`, etc. on Unix based OS-es, and using the `set` command in Windows.

`example/01-etcd-config.yaml` has the configuration required to set up `etcdbrctl` while running `etcd` as a process alongside `etcdbrctl`.
`POD_NAME`, and `POD_NAMESPACE` are environment variables that `etcdbrctl` expects since it is typically run as a sidecar container alongside a [`etcd`](https://github.com/gardener/etcd-wrapper) in a pod, as an instance of the `Etcd` CR as defined in [`etcd-druid`](https://github.com/gardener/etcd-druid). These values are provided to ensure `etcdbrctl` runs.

### Credential structure

#### Credentials in a directory

Taking the example of S3 again here, the following is the necessary structure for a directory which holds the S3 credential files (extra credentials correspond to extra files, which are not compulsory).

```console
s3credentials
├── accessKeyID
├── region
└── secretAccessKey
```

> [!TIP]  
> Each file mentioned above will consist of the credential strings only. Ensure that there are no newline/carriage return characters at the end of the files while saving these credential files. Modern text editors are typically configured to add newline characters at the end of files on format/save. The newline characters will completely change the credentials and will cause the requests with S3 to not get authenticated. For example, the region file would contain only the string eu-west-1, without any trailing characters.

#### Credentials in a JSON file

Sticking to the S3 example, the `.json` file which contains the credentials should look like so:

```console
{
  "accessKeyID": "<accessKeyID>",
  "region": "<region>",
  "secretAccessKey": "<secretAccessKey>",
}
```
