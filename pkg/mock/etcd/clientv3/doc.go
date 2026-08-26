// SPDX-FileCopyrightText: Copyright Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0
//go:generate mockgen -package clientv3 -destination=mocks.go go.etcd.io/etcd/client/v3 Watcher

package clientv3
