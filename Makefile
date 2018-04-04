# Copyright 2018 The Gardener Authors.
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

VCS                 := github.com
ORGANIZATION        := gardener
PROJECT             := etcd-backup-restore
REPOSITORY          := $(VCS)/$(ORGANIZATION)/$(PROJECT)
VERSION             := $(shell cat VERSION)
LD_FLAGS            := "-w -X $(REPOSITORY)/pkg/version.Version=$(VERSION)"
PACKAGES            := $(shell go list ./... | grep -vE '/vendor/')
LINT_FOLDERS        := $(shell echo $(PACKAGES) | sed "s|$(REPOSITORY)|.|g")
TEST_FOLDERS        := cmd pkg
REGISTRY            := docker.io/gardener/etcdbr
IMAGE_REPOSITORY    := $(REGISTRY)/etcdbrctl
IMAGE_TAG           := $(VERSION)

BUILD_DIR        := build
BIN_DIR          := bin
GOBIN            := $(PWD)/$(BIN_DIR)
GO_EXTRA_FLAGS   := -v -a
PATH             := $(GOBIN):$(PATH)
USER             := $(shell id -u -n)

export PATH
export GOBIN

.PHONY: dev
dev: 
	@go build -o $(BIN_DIR)/etcdbrctl $(GO_EXTRA_FLAGS) -ldflags $(LD_FLAGS) main.go
	
.PHONY: verify
verify: vet fmt lint test

.PHONY: revendor
revendor:
	@dep ensure -update -v

.PHONY: build
build: 
	@env GOOS=linux GOARCH=amd64 go build  -o $(BIN_DIR)/linux-amd64/etcdbrctl $(GO_EXTRA_FLAGS) -ldflags $(LD_FLAGS) main.go

.PHONY: docker-image
docker-image: 
	@if [[ ! -f $(BIN_DIR)/linux-amd64/etcdbrctl ]]; then echo "No binary found. Please run 'make build'"; false; fi
	@docker build -t $(IMAGE_REPOSITORY):$(IMAGE_TAG) -f $(BUILD_DIR)/Dockerfile --rm .

.PHONY: docker-push
docker-push:
	@if ! docker images $(IMAGE_REPOSITORY) | awk '{ print $$2 }' | grep -q -F $(IMAGE_TAG); then echo "$(IMAGE_REPOSITORY) version $(IMAGE_TAG) is not yet built. Please run 'make docker-image'"; false; fi
	@docker push $(IMAGE_REPOSITORY):$(IMAGE_TAG)

.PHONY: clean
clean:
	@rm -rf $(BIN_DIR)/

.PHONY: fmt
fmt:
	@go fmt $(PACKAGES)

.PHONY: vet
vet:
	@go vet $(PACKAGES)

.PHONY: lint
lint:
	@golint  --set_exit_status $(LINT_FOLDERS)

.PHONY: test
test:
	@ginkgo -r $(TEST_FOLDERS)
