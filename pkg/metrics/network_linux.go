// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

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
	fd, err := os.Open(fileName) // #nosec #G304 -- trusted file to read network usage stats.
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
