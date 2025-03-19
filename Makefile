# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

VERSION             ?= $(shell cat VERSION)
REPO_ROOT           := $(shell dirname "$(realpath $(lastword $(MAKEFILE_LIST)))")
REGISTRY            ?= europe-docker.pkg.dev/gardener-project/snapshots
IMAGE_REPOSITORY    := $(REGISTRY)/gardener/etcdbrctl
IMAGE_TAG           := $(VERSION)
BUILD_DIR           := build
PLATFORM            ?= $(shell docker info --format '{{.OSType}}/{{.Architecture}}')
BIN_DIR             := bin
COVERPROFILE        := test/output/coverprofile.out
IMG                 ?= ${IMAGE_REPOSITORY}:${IMAGE_TAG}
KUBECONFIG_PATH     :=$(REPO_ROOT)/hack/e2e-test/infrastructure/kind/kubeconfig

.DEFAULT_GOAL := build-local

include $(REPO_ROOT)/hack/tools.mk

.PHONY: revendor
revendor:
	@env go mod tidy -v
	@env go mod vendor -v

.PHONY: update-dependencies
update-dependencies:
	@env go get -u
	@make revendor

kind-up kind-down ci-e2e-kind ci-e2e-kind-aws ci-e2e-kind-azure ci-e2e-kind-gcp: export KUBECONFIG = $(KUBECONFIG_PATH)

.PHONY: build
build:
	@.ci/build

.PHONY: build-local
build-local:
	@env LOCAL_BUILD=1 .ci/build

.PHONY: docker-build
docker-build:
	docker buildx build --platform=$(PLATFORM) --tag $(IMG) -f $(BUILD_DIR)/Dockerfile --rm .

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
check: $(HELM)
	@.ci/check

.PHONY: sast
sast: $(GOSEC)
	@./hack/sast.sh

.PHONY: sast-report
sast-report: $(GOSEC)
	@./hack/sast.sh --gosec-report true

.PHONY: test
test:
	.ci/unit_test

.PHONY: perf-regression-test
perf-regression-test:
	@.ci/performance_regression_test

.PHONY: integration-test
integration-test:
	@.ci/integration_test

.PHONY: show-coverage
show-coverage:
	@if [ ! -f $(COVERPROFILE) ]; then echo "$(COVERPROFILE) is not yet built. Please run 'COVER=true make test'"; false; fi
	@go tool cover -html $(COVERPROFILE)

.PHONY: test-e2e
test-e2e: $(KIND) $(HELM) $(GINKGO) $(KUBECTL)
	@"$(REPO_ROOT)/hack/e2e-test/run-e2e-test.sh" $(PROVIDERS) $(KUBECONFIG)

.PHONY: kind-up
kind-up: $(KIND)
	./hack/kind-up.sh

.PHONY: kind-down
kind-down: $(KIND)
	kind delete cluster --name etcdbr-e2e

.PHONY: deploy-localstack
deploy-localstack: $(KUBECTL)
	./hack/deploy-localstack.sh $(KUBECONFIG)

.PHONY: deploy-fakegcs
deploy-fakegcs: $(KUBECTL)
	./hack/deploy-fakegcs.sh $(KUBECONFIG)

.PHONY: deploy-azurite
deploy-azurite: $(KUBECTL)
	./hack/deploy-azurite.sh $(KUBECONFIG)

.PHONY: ci-e2e-kind
ci-e2e-kind:
	./hack/ci-e2e-kind.sh $(PROVIDERS)

.PHONY: ci-e2e-kind-aws
ci-e2e-kind-aws:
	./hack/ci-e2e-kind.sh aws

.PHONY: ci-e2e-kind-azure
ci-e2e-kind-azure:
	./hack/ci-e2e-kind.sh azure

.PHONY: ci-e2e-kind-gcp
ci-e2e-kind-gcp:
	./hack/ci-e2e-kind.sh gcp
