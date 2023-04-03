# Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved. This file is licensed under the Apache Software License, v. 2 except as noted otherwise in the LICENSE file.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

VERSION             := $(shell cat VERSION)
REGISTRY            ?= eu.gcr.io/gardener-project/gardener
IMAGE_REPOSITORY    := $(REGISTRY)/etcdbrctl
IMAGE_TAG           := $(VERSION)
BUILD_DIR           := build
BIN_DIR             := bin
COVERPROFILE        := test/output/coverprofile.out

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
