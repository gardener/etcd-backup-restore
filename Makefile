# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

VERSION             ?= $(shell cat VERSION)
REGISTRY            ?= europe-docker.pkg.dev/gardener-project/public
IMAGE_REPOSITORY    := $(REGISTRY)/gardener/etcdbrctl
IMAGE_TAG           := $(VERSION)
BUILD_DIR           := build
BIN_DIR             := bin
COVERPROFILE        := test/output/coverprofile.out

IMG ?= ${IMAGE_REPOSITORY}:${IMAGE_TAG}

.DEFAULT_GOAL := build-local

.PHONY: revendor
revendor:
	@env go mod tidy -v
	@env go mod vendor -v

.PHONY: update-dependencies
update-dependencies:
	@env go get -u
	@make revendor

.PHONY: build
build:
	@.ci/build

.PHONY: build-local
build-local:
	@env LOCAL_BUILD=1 .ci/build

.PHONY: docker-build
docker-build:
	@docker build -t ${IMG} -f $(BUILD_DIR)/Dockerfile --rm .

.PHONY: docker-push
docker-push:
	@if ! docker images $(IMAGE_REPOSITORY) | awk '{ print $$2 }' | grep -q -F $(IMAGE_TAG); then echo "$(IMAGE_REPOSITORY) version $(IMAGE_TAG) is not yet built. Please run 'make docker-image'"; false; fi
	@docker push ${IMG}

.PHONY: clean
clean:
	@rm -rf $(BIN_DIR)/

.PHONY: verify
verify: check test

.PHONY: check
check:
	@.ci/check

.PHONY: test
test:
	.ci/unit_test

.PHONY: perf-regression-test
perf-regression-test:
	@.ci/performance_regression_test

.PHONY: integration-test
integration-test:
	@.ci/integration_test

.PHONY: integration-test-cluster
integration-test-cluster:
	@.ci/integration_test cluster

.PHONY: show-coverage
show-coverage:
	@if [ ! -f $(COVERPROFILE) ]; then echo "$(COVERPROFILE) is not yet built. Please run 'COVER=true make test'"; false; fi
	@go tool cover -html $(COVERPROFILE)
