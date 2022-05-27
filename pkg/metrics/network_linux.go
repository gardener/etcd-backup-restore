// Copyright 2019 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux
// +build linux

package metrics

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type networkMode string

const (
	transmitNetworkMode networkMode = "transmit"
	receiveNetworkMode  networkMode = "receive"
)

func getNetworkUsageBytes(mode networkMode) float64 {
	var bytesField int
	switch mode {
	case transmitNetworkMode:
		bytesField = 8
	case receiveNetworkMode:
		bytesField = 0
	}

	pid := os.Getpid()
	fileName := fmt.Sprintf("/proc/%d/net/dev", pid)
	fd, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("failed to readfile: %s", fileName)
		return math.NaN()
	}
	scanner := bufio.NewScanner(fd)
	var usageBytes float64
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ":")
		if len(fields) < 2 {
			// Ignore header line
			continue
		}
		value := strings.Fields(fields[1])
		ifaceBytes, err := strconv.ParseFloat(value[bytesField], 64)
		if err != nil {
			return math.NaN()
		}
		usageBytes = usageBytes + ifaceBytes
	}
	return usageBytes
}

func getNetworkTransmittedBytes() float64 {
	return getNetworkUsageBytes(transmitNetworkMode)
}

func getNetworkReceivedBytes() float64 {
	return getNetworkUsageBytes(receiveNetworkMode)
}

var (
	networkTransmittedBytes = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: namespaceEtcdBR,
			Subsystem: "network",
			Name:      "transmitted_bytes",
			Help:      "Number of bytes transmitted over network.",
		},
		getNetworkTransmittedBytes,
	)
	networkReceivedBytes = prometheus.NewCounterFunc(
		prometheus.CounterOpts{
			Namespace: namespaceEtcdBR,
			Subsystem: "network",
			Name:      "received_bytes",
			Help:      "Number of bytes received over network.",
		},
		getNetworkReceivedBytes,
	)
)

func init() {
	prometheus.MustRegister(networkTransmittedBytes)
	prometheus.MustRegister(networkReceivedBytes)
}
