# SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
#
# SPDX-License-Identifier: Apache-2.0

FROM golang:1.23.6 as builder

WORKDIR /go/src/github.com/gardener/backup-restore
COPY . .

RUN make build

FROM gcr.io/distroless/static-debian11:nonroot AS backup-restore

COPY --from=builder /go/src/github.com/gardener/backup-restore/bin/etcdbrctl /etcdbrctl
WORKDIR /
ENTRYPOINT ["/etcdbrctl"]
