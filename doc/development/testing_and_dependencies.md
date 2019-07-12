# Dependency management

We use golang modules to manage golang dependencies. In order to add a new package dependency to the project, you can perform `go get <PACKAGE>@<VERSION>` or edit the `go.mod` file and append the package along with the version you want to use.

### Updating dependencies

The `Makefile` contains a rule called `revendor` which performs `go mod vendor` and `go mod tidy`.
* `go mod vendor` resets the main module's vendor directory to include all packages needed to build and test all the main module's packages. It does not include test code for vendored packages.
It does not include test code for vendored packages.
* `go mod tidy` makes sure go.mod matches the source code in the module.
It adds any missing modules necessary to build the current module's
packages and dependencies, and it removes unused modules that
don't provide any relevant packages.

```sh
make revendor
```

The dependencies are installed into the `vendor` folder which **should be added** to the VCS.

:warning: Make sure you test the code after you have updated the dependencies!

# Testing

We have created `make` target `verify` which will internally run different rule like `fmt` for formatting, `lint` for linting check and most importantly `test` which will check the code against predefined unit tests. Although, currently there are not enough test cases written to cover entire code, hence one should check for failure cases manually before raising pull request. We will eventually add the test cases for complete code coverage.

```sh
make verify
```

By default, we try to run test in parallel without computing code coverage. To get the code coverage, you will have to set environment variable `COVER` to `true`. This will log the code coverage percentage at the end of test logs. Also, all cover profile files will accumulated under `test/output/coverprofile.out` directory. You can visualize exact code coverage using `make show-coverage`.
