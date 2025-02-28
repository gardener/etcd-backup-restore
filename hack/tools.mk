# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

TOOLS_DIR     := hack/tools
TOOLS_BIN_DIR := $(TOOLS_DIR)/bin

export TOOLS_BIN_DIR := $(TOOLS_BIN_DIR)
export PATH := $(abspath $(TOOLS_BIN_DIR)):$(PATH)

# Tool Binaries
GOSEC   := $(TOOLS_BIN_DIR)/gosec
GINKGO  := $(TOOLS_BIN_DIR)/ginkgo
HELM    := $(TOOLS_BIN_DIR)/helm
KIND    := $(TOOLS_BIN_DIR)/kind
KUBECTL := $(TOOLS_BIN_DIR)/kubectl

# Tool Versions
GOSEC_VERSION ?= v2.21.4
HELM_VERSION ?= v3.13.1
KIND_VERSION ?= v0.20.0
KUBECTL_VERSION ?= v1.28.3


# Use this "function" to add the version file as a prerequisite for the tool target: e.g.
#   $(HELM): $(call tool_version_file,$(HELM),$(HELM_VERSION))
tool_version_file = $(TOOLS_BIN_DIR)/.version_$(subst $(TOOLS_BIN_DIR)/,,$(1))_$(2)

# This target cleans up any previous version files for the given tool and creates the given version file.
# This way, we can generically determine, which version was installed without calling each and every binary explicitly.
$(TOOLS_BIN_DIR)/.version_%:
	@version_file=$@; rm -f $${version_file%_*}*
	@touch $@

#########################################
# Tools                                 #
#########################################

$(GOSEC): $(call tool_version_file,$(GOSEC),$(GOSEC_VERSION))
	@GOSEC_VERSION=$(GOSEC_VERSION) $(TOOLS_DIR)/install-gosec.sh
	
$(GINKGO): go.mod
	go build -o $(GINKGO) github.com/onsi/ginkgo/v2/ginkgo

$(KUBECTL): $(call tool_version_file,$(KUBECTL),$(KUBECTL_VERSION))
	curl -Lo $(KUBECTL) https://dl.k8s.io/release/$(KUBECTL_VERSION)/bin/$(shell uname -s | tr '[:upper:]' '[:lower:]')/$(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')/kubectl
	chmod +x $(KUBECTL)

$(HELM): $(call tool_version_file,$(HELM),$(HELM_VERSION))
	curl -sSfL https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | HELM_INSTALL_DIR=$(TOOLS_BIN_DIR) USE_SUDO=false bash -s -- --version $(HELM_VERSION)

$(KIND): $(call tool_version_file,$(KIND),$(KIND_VERSION))
	curl -L -o $(KIND) https://kind.sigs.k8s.io/dl/$(KIND_VERSION)/kind-$(shell uname -s | tr '[:upper:]' '[:lower:]')-$(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
	chmod +x $(KIND)
	