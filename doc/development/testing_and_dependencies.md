# Dependency management

We use go-modules to manage golang dependencies. In order to add a new package dependency to the project, you can perform `go get <PACKAGE>@<VERSION>` or edit the `go.mod` file and append the package along with the version you want to use.

### Updating dependencies

The `Makefile` contains a rule called `revendor` which performs `go mod vendor` and `go mod tidy`.
* `go mod vendor` resets the main module's vendor directory to include all packages needed to build and test all the main module's packages. It does not include test code for vendored packages.
* `go mod tidy` makes sure go.mod matches the source code in the module.
It adds any missing modules necessary to build the current module's packages and dependencies, and it removes unused modules that don't provide any relevant packages.

```sh
make revendor
```

The dependencies are installed into the `vendor` folder which **should be added** to the VCS.

:warning: Make sure you test the code after you have updated the dependencies!

# Testing

We have created `make` target `verify` which will internally run different rules like `fmt` for formatting, `lint` for linting check and most importantly `test` which will check the code against predefined unit tests. As currently there aren't enough test cases written to cover the entire code, you must check for failure cases manually and include test cases before raising pull request. We will eventually add more test cases for complete code coverage.

```sh
make verify
```

By default, we run tests without computing code coverage. To get the code coverage, you can set the environment variable `COVER` to `true`. This will log the code coverage percentage at the end of test logs. Also, all cover profile files will be accumulated under `test/output/coverprofile.out` directory. You can visualize the exact code coverage by running `make show-coverage` after running `make verify` with code coverage enabled.
