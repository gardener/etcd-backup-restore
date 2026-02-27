# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

VERSION             ?= $(shell cat VERSION)
GIT_SHA             := $(shell git rev-parse --short HEAD || echo "GitNotFound")
REPO_ROOT           := $(shell dirname "$(realpath $(lastword $(MAKEFILE_LIST)))")
REGISTRY            ?= europe-docker.pkg.dev/gardener-project/snapshots
IMAGE_REPOSITORY    := $(REGISTRY)/gardener/etcdbrctl
REPOSITORY          ?= "github.com/gardener/etcd-backup-restore"
IMAGE_TAG           := $(VERSION)
BUILD_DIR           := build
PLATFORM            ?= $(shell docker info --format '{{.OSType}}/{{.Architecture}}')
BINARY_DIR          ?= bin
COVERPROFILE        := test/output/coverprofile.out
IMG                 ?= ${IMAGE_REPOSITORY}:${IMAGE_TAG}
KUBECONFIG_PATH     := $(REPO_ROOT)/hack/e2e-test/infrastructure/kind/kubeconfig

.DEFAULT_GOAL := build

# Tools
# -------------------------------------------------------------------------
include $(REPO_ROOT)/hack/tools.mk

# Rules for verification, formatting, linting and cleaning
# -------------------------------------------------------------------------
# Add license headers.
.PHONY: add-license-headers
add-license-headers: $(GO_ADD_LICENSE)
	@./hack/addlicenseheaders.sh ${YEAR}

# Format code and arrange imports.
.PHONY: format
format: $(GOIMPORTS_REVISER)
	@./hack/format.sh ./cmd/ ./pkg/ ./test/

# Check packages
.PHONY: check
check: $(GOLANGCI_LINT) $(GOIMPORTS) $(HELM) format
	@./hack/check.sh --golangci-lint-config=./.golangci.yaml ./pkg/... ./cmd/... ./test/...

.PHONY: sast
sast: $(GOSEC)
	@./hack/sast.sh

.PHONY: sast-report
sast-report: $(GOSEC)
	@./hack/sast.sh --gosec-report true

.PHONY: revendor
revendor:
	@env go mod tidy -v
	@env go mod vendor -v

.PHONY: update-dependencies
update-dependencies:
	@env go get -u
	@make revendor

.PHONY: verify
verify: check test-unit

.PHONY: clean
clean:
	@rm -rf $(BINARY_DIR)/

# Rules for testing (unit, integration and end-2-end)
# -------------------------------------------------------------------------
# Run tests
.PHONY: test-unit
test-unit: $(GINKGO)
	@./hack/test-unit.sh

.PHONY: perf-regression-test
perf-regression-test:
	@.ci/performance_regression_test

.PHONY: test-integration
test-integration: $(GINKGO)
	@./hack/test-integration.sh

.PHONY: show-coverage
show-coverage:
	@if [ ! -f $(COVERPROFILE) ]; then echo "$(COVERPROFILE) is not yet built. Please run 'COVER=true make test'"; false; fi
	@go tool cover -html $(COVERPROFILE)

.PHONY: test-e2e
test-e2e: $(KIND) $(HELM) $(GINKGO) $(KUBECTL)
	@"$(REPO_ROOT)/hack/e2e-test/run-e2e-test.sh" $(PROVIDERS) $(KUBECONFIG)

.PHONY: ci-e2e-kind
ci-e2e-kind:
	@./hack/ci-e2e-kind.sh $(PROVIDERS)

.PHONY: ci-e2e-kind-aws
ci-e2e-kind-aws:
	@./hack/ci-e2e-kind.sh aws

.PHONY: ci-e2e-kind-azure
ci-e2e-kind-azure:
	@./hack/ci-e2e-kind.sh azure

.PHONY: ci-e2e-kind-gcp
ci-e2e-kind-gcp:
	@./hack/ci-e2e-kind.sh gcp

# Rules related to binary build, Docker image build and release
# -------------------------------------------------------------------------
.PHONY: build
build:
	@CGO_ENABLED=0  go build \
	  -v \
	  -mod vendor \
	  -o "${BINARY_DIR}/etcdbrctl" \
	  -ldflags "-w -X ${REPOSITORY}/pkg/version.Version=${VERSION} -X ${REPOSITORY}/pkg/version.GitSHA=${GIT_SHA}" \
	  main.go

.PHONY: docker-build
docker-build:
	@docker buildx build --platform=$(PLATFORM) --tag $(IMG) -f $(BUILD_DIR)/Dockerfile --rm .

.PHONY: docker-push
docker-push:
	@if ! docker images $(IMAGE_REPOSITORY) | awk '{ print $$2 }' | grep -q -F $(IMAGE_TAG); then echo "$(IMAGE_REPOSITORY) version $(IMAGE_TAG) is not yet built. Please run 'make docker-image'"; false; fi
	@docker push ${IMG}

# Rules for local/remote environment
# -------------------------------------------------------------------------
kind-up kind-down ci-e2e-kind ci-e2e-kind-aws ci-e2e-kind-azure ci-e2e-kind-gcp: export KUBECONFIG = $(KUBECONFIG_PATH)

.PHONY: kind-up
kind-up: $(KIND)
	@./hack/kind-up.sh
	@printf "\n\033[0;33m📌 NOTE: To target the newly created kind cluster, please run the following command:\n\n    export KUBECONFIG=$(KUBECONFIG_PATH)\n\033[0m\n"

.PHONY: kind-down
kind-down: $(KIND)
	@kind delete cluster --name etcdbr-e2e

.PHONY: deploy-localstack
deploy-localstack: $(KUBECTL)
	@./hack/deploy-localstack.sh $(KUBECONFIG)

.PHONY: deploy-fakegcs
deploy-fakegcs: $(KUBECTL)
	@./hack/deploy-fakegcs.sh $(KUBECONFIG)

.PHONY: deploy-azurite
deploy-azurite: $(KUBECTL)
	@./hack/deploy-azurite.sh $(KUBECONFIG)
