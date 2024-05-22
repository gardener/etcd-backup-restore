# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

VERSION             ?= $(shell cat VERSION)
REPO_ROOT           := $(shell dirname "$(realpath $(lastword $(MAKEFILE_LIST)))")
REGISTRY            ?= europe-docker.pkg.dev/gardener-project/snapshots
IMAGE_REPOSITORY    := $(REGISTRY)/gardener/etcdbrctl
IMAGE_TAG           := $(VERSION)
BUILD_DIR           := build
BIN_DIR             := bin
COVERPROFILE        := test/output/coverprofile.out
KUBECONFIG_PATH     :=$(REPO_ROOT)/hack/e2e-test/infrastructure/kind/kubeconfig

IMG ?= ${IMAGE_REPOSITORY}:${IMAGE_TAG}

.DEFAULT_GOAL := build-local

.PHONY: revendor
revendor:
	@env GO111MODULE=on go mod tidy -v
	@env GO111MODULE=on go mod vendor -v

.PHONY: update-dependencies
update-dependencies:
	@env GO111MODULE=on go get -u
	@make revendor

kind-up kind-down ci-e2e-kind deploy-localstack deploy-fakegcs deploy-azurite test-e2e: export KUBECONFIG = $(KUBECONFIG_PATH)

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

.PHONY: test-e2e
test-e2e: $(KUBECTL) $(HELM) $(SKAFFOLD)
	@"$(REPO_ROOT)/hack/e2e-test/run-e2e-test.sh" $(PROVIDERS)

.PHONY: kind-up
kind-up: $(KIND)
	./hack/kind-up.sh

.PHONY: kind-down
kind-down: $(KIND)
	$(KIND) delete cluster --name etcdbr-e2e

.PHONY: deploy-localstack
deploy-localstack: $(KUBECTL)
	./hack/deploy-localstack.sh

.PHONY: deploy-fakegcs
deploy-fakegcs: $(KUBECTL)
	./hack/deploy-fakegcs.sh

.PHONY: deploy-azurite
deploy-azurite: $(KUBECTL)
	./hack/deploy-azurite.sh

.PHONY: ci-e2e-kind
ci-e2e-kind:
	./hack/ci-e2e-kind.sh

.PHONY: pr-test-e2e
pr-test-e2e:
	./hack/ci-e2e-kind.sh aws

.PHONY: merge-test-e2e
merge-test-e2e:
	./hack/ci-e2e-kind.sh aws,gcp,azure
